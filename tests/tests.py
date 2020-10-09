#!/usr/bin/env python3

from sources.patchwork import Patchwork
from sources.github_sync import GithubSync
import mock
from mock import Mock, patch
from munch import Munch
from datetime import datetime
import unittest
import json
import copy
from freezegun import freeze_time


class TestGhSync(unittest.TestCase):
    def pw_get_mock(self, req):
        # do not emulate pagination
        m = Mock(json=Mock(return_value=self.pw_fixtures[req]), headers={})
        return m

    def mock_pw(self):
        with open("./tests/pw_fixtures.json", "r") as f:
            self.pw_fixtures = json.load(f)
        self.mock_pw_patcher = patch("patchwork.Patchwork._get", side_effect=self.pw_get_mock).start()
        patch("patchwork.Patchwork.get_blob", return_value=b"Dummy data").start()
        self.mock_pw_time()

    def mock_pw_time(self):
        self.freezer = freeze_time(datetime.fromtimestamp(1602193333)).start()
        #patch("sources.patchwork.Patchwork.format_since", return_value="2020-09-28T00:00:00").start()
        #patch("datetime.datetime.now", return_value=datetime.datetime.fromtimestamp(1601934111)).start()

    def gh_pulls_mock(self, *args, **kwargs):
        #mock github get_pulls method
        r = []
        for pr in self.gh_prs:
            if len(kwargs) == 0:
                # we're not filtering
                r.append(pr)
                continue
            match = True
            for key, value in kwargs.items():
                if (
                    (key == "base" and pr.base.ref != value)
                    or
                    (key != "base" and getattr(pr, key) != value)
                ):
                    match = False
                    break
            if match:
                r.append(pr)
        return r

    def gh_get_branches(self):
        # mock gh get_granches method
        b = set()
        for pr in self.gh_prs:
            b.add(pr.head.ref)
        return b

    def mock_pr_close(self, pr):
        # do not update status, just remember it
        self.closed_prs.append(pr)

    def mock_pr_methods(self, pr):
        pr.create_issue_comment = Mock()
        #pr.close

    def mock_sync_tags(self, pr, labels):
        pr.labels = labels

    def create_pull_mock(self, *args, **kwargs):
        # create pull request method mock
        self.prn += 1
        data = copy.copy(kwargs)
        data["state"] = "open"
        for key in ["head", "base"]:
            data[key] = {
                "ref": kwargs[key],
                "user": {"login": self.gh_mock().get_user().login}
            }

        m = Munch.fromDict(data)
        self.mock_pr_methods(m)
        m.number = self.prn
        self.created_prs.append(m)
        return m


    def mock_gh(self):
        # create all mocks relateted to Github
        with open("./tests/gh_fixtures.json", "r") as f:
            self.gh_fixtures = json.load(f)
            self.gh_prs = []
            self.prn = 0
            for pr in self.gh_fixtures:
                m = Munch.fromDict(self.gh_fixtures[pr])
                m.number = int(pr)
                self.mock_pr_methods(m)
                self.gh_prs.append(m)
                self.prn = max(self.prn, m.number)
        #get_user = Mock(get_repo=Mock(return_value=get_repo))
        #gh = Mock(get_user=get_user)
        #gh_mock = patch("sources.github_sync.Github", return_value=gh)
        self.close_mock = patch("sources.github_sync.GithubSync._close_pr", side_effect=self.mock_pr_close).start()
        self.tag_pr_mock = patch("sources.github_sync.GithubSync._sync_pr_tags", side_effect=self.mock_sync_tags).start()
        self.gh_mock = patch("sources.github_sync.Github").start()
        self.gh_mock().get_user().login = "tsipa"
        self.gh_mock().get_user().get_repo().get_branches.side_effect = self.gh_get_branches
        self.gh_mock().get_user().get_repo().get_pulls.side_effect = self.gh_pulls_mock
        self.gh_mock().get_user().get_repo().create_pull.side_effect = self.create_pull_mock


    def mock_local_git(self):
        self.git_mock = patch("sources.github_sync.GithubSync.fetch_repo").start()
        self.git_am_mock = self.git_mock().git.am
        self.git_diff_mock = self.git_mock().git.diff
        self.git_diff_mock.return_value = ""
        patch("sources.github_sync.os.system").start()


    def dummy_worker(self):
            return GithubSync(
                pw_url="http://pw.dummyhost.com/api/",
                pw_search_patterns=[{"archived": False, "project": 399, "delegate": 121173}],
                pw_lookback=7,
                master="bpf-next",
                repo_url="git@github.com:tsipa/bpf-next",
                github_oauth_token="GKJHLASDLJHKGASDGLUHDSGHASDGHVDSALKJG",
                sync_from="https://git.kernel.org/pub/scm/linux/kernel/git/bpf/bpf-next.git",
                source_master="master",
                ci_repo="git@github.com:tsipa/vmtest",
                ci_branch="master",
                filter_tags=["bpf"],
            )

    def setUp(self):
        self.reset_mocks()

    def reset_mocks(self):
        self.mock_pw()
        self.mock_gh()
        self.mock_local_git()
        self.worker = self.dummy_worker()
        self.created_prs = []
        self.closed_prs = []

    def get_commented_prs(self):
        commented_prs = []
        for pr in self.gh_prs:
            if pr.create_issue_comment.call_count != 0:
                commented_prs.append(pr)
        return commented_prs

    def get_baseline(self):
        """
            Returns tuple of commented_prs, created_prs, closed_prs
            and reset status quo of instance-wide mocks
        """
        self.worker.sync_branches()
        ret = self._get_current_stats()
        self.reset_mocks()
        return ret

    def _get_current_stats(self):
        commented_prs = copy.copy(self.get_commented_prs())
        created_prs = copy.copy(self.created_prs)
        closed_prs = copy.copy(self.closed_prs)
        return (commented_prs, created_prs, closed_prs)



    def test_create_pr(self):
        """
            - New PR has to be created if it doesn't exist
        """
        commented_prs, created_prs, closed_prs = self.get_baseline()

        open_prs = self.gh_pulls_mock(state="open")
        self.gh_prs.remove(open_prs[0])
        self.worker.sync_branches()
        new_commented_prs, new_created_prs, new_closed_prs = self._get_current_stats()
        self.assertEqual(len(new_created_prs) - len(created_prs), 1)
        self.assertEqual(commented_prs, new_commented_prs)
        self.assertEqual(closed_prs, new_closed_prs)

    def test_comment_pr(self):
        """
            - Every time we have a diff for existing PR we must add a comment
        """
        commented_prs, created_prs, closed_prs = self.get_baseline()

        # all diffs but first must return ""
        diffs = ["Dummy diff content"]
        for d in self.gh_prs:
            diffs.append("")
        self.git_diff_mock.side_effect = diffs
        self.worker.sync_branches()
        new_commented_prs, new_created_prs, new_closed_prs = self._get_current_stats()
        self.assertEqual(len(new_commented_prs) - len(commented_prs), 1)
        self.assertEqual(new_created_prs, created_prs)
        self.assertEqual(closed_prs, new_closed_prs)


    def test_
        import pdb; pdb.set_trace()


if __name__ == '__main__':
    unittest.main()

