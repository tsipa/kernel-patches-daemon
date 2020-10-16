#!/usr/bin/env python3

from sources.patchwork import Patchwork, Series
from sources.github_sync import GithubSync
import mock
from mock import Mock, patch, PropertyMock
from munch import Munch
from datetime import datetime
import unittest
import json
import copy
from freezegun import freeze_time

FREEZE_TIME = 1602632850
TEST_BASE_BRANCH = "bpf-next"


class TestGhSync(unittest.TestCase):
    def pw_get_mock(self, req):
        # do not emulate pagination
        m = Mock(json=Mock(return_value=self.pw_fixtures[req]), headers={})
        return m

    def mock_pw(self):
        with open("./tests/pw_fixtures.json", "r") as f:
            self.pw_fixtures = json.load(f)
        self.mock_pw_patcher = patch(
            "patchwork.Patchwork._get", side_effect=self.pw_get_mock
        ).start()
        patch("patchwork.Patchwork.get_blob", return_value=b"Dummy data").start()
        self.mock_pw_time()

    def mock_pw_time(self):
        self.freezer = freeze_time(datetime.fromtimestamp(FREEZE_TIME)).start()
        # patch("sources.patchwork.Patchwork.format_since", return_value="2020-09-28T00:00:00").start()
        # patch("datetime.datetime.now", return_value=datetime.datetime.fromtimestamp(1601934111)).start()

    def gh_pulls_mock(self, *args, **kwargs):
        # mock github get_pulls method
        r = []
        for pr in self.gh_prs:
            if len(kwargs) == 0:
                # we're not filtering
                r.append(pr)
                continue
            match = True
            for key, value in kwargs.items():
                if (key == "base" and pr.base.ref != value) or (
                    key != "base" and getattr(pr, key) != value
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
            if pr.state == "open":
                m = Mock(pr.head.ref, name=pr.head.ref)
                m.name = pr.head.ref
                b.add(m)
        for extra_branch in self.extra_branches:
            m = Mock(extra_branch, name=extra_branch)
            m.name = extra_branch
            b.add(m)
        return b

    def mock_pr_close(self, pr):
        # do not update status, just remember it
        pr.state = "closed"
        self.closed_prs.append(pr)

    def mock_pr_reopen(self, pr):
        pr.state = "open"
        self.reopened_prs.append(pr)

    def mock_pr_methods(self, pr):
        pr.create_issue_comment = Mock()
        # pr.close

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
                "user": {"login": self.gh_mock().get_user().login},
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
                data = copy.copy(self.gh_fixtures[pr])
                del data["updated_at"]
                m = Munch.fromDict(data)
                m.number = int(pr)
                m.updated_at = Mock()
                m.updated_at.timestamp.return_value = self.gh_fixtures[pr]["updated_at"]
                self.mock_pr_methods(m)
                self.gh_prs.append(m)
                self.prn = max(self.prn, m.number)
        # get_user = Mock(get_repo=Mock(return_value=get_repo))
        # gh = Mock(get_user=get_user)
        # gh_mock = patch("sources.github_sync.Github", return_value=gh)
        self.close_mock = patch(
            "sources.github_sync.GithubSync._close_pr", side_effect=self.mock_pr_close
        ).start()
        self.reopen_mock = patch(
            "sources.github_sync.GithubSync._reopen_pr", side_effect=self.mock_pr_reopen
        ).start()
        self.tag_pr_mock = patch(
            "sources.github_sync.GithubSync._sync_pr_tags",
            side_effect=self.mock_sync_tags,
        ).start()
        self.gh_mock = patch("sources.github_sync.Github").start()
        self.gh_mock().get_user().login = "tsipa"
        self.gh_mock().get_user().get_repo().get_branches.side_effect = (
            self.gh_get_branches
        )
        self.gh_mock().get_user().get_repo().get_pulls.side_effect = self.gh_pulls_mock
        self.gh_mock().get_user().get_repo().create_pull.side_effect = (
            self.create_pull_mock
        )
        self.branch_delete_mock = (
            self.gh_mock().get_user().get_repo().get_git_ref().delete
        )

    def mock_local_git(self):
        self.git_mock = patch("sources.github_sync.GithubSync.fetch_repo").start()
        self.git_am_mock = self.git_mock().git.am
        self.git_diff_mock = self.git_mock().git.diff

        self.git_diff_mock.return_value = ""
        patch("sources.github_sync.os.system").start()

    def dummy_worker(self):
        return GithubSync(
            pw_url="http://pw.dummyhost.com/api/",
            pw_search_patterns=[
                {"archived": False, "project": 399, "delegate": 121173}
            ],
            pw_lookback=7,
            master=TEST_BASE_BRANCH,
            repo_url="git@github.com:tsipa/bpf-next",
            github_oauth_token="GKJHLASDLJHKGASDGLUHDSGHASDGHVDSALKJG",
            sync_from="https://git.kernel.org/pub/scm/linux/kernel/git/bpf/bpf-next.git",
            source_master="master",
            ci_repo="git@github.com:tsipa/vmtest",
            ci_branch="master",
            filter_tags=["bpf"],
        )

    def reset_mocks(self):
        self.extra_branches = []
        self.mock_pw()
        self.mock_gh()
        self.mock_local_git()
        self.worker = self.dummy_worker()
        self.created_prs = []
        self.closed_prs = []
        self.reopened_prs = []

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
        self.reset_mocks()
        self.worker.sync_branches()
        ret = self._get_current_stats()
        self.reset_mocks()
        return ret

    def _get_current_stats(self):
        stats = {}
        stats["commented_prs"] = copy.copy(self.get_commented_prs())
        stats["created_prs"] = copy.copy(self.created_prs)
        stats["closed_prs"] = copy.copy(self.closed_prs)
        stats["deleted_branches"] = copy.copy(self.branch_delete_mock.call_count)
        stats["reopened_prs"] = copy.copy(self.reopened_prs)
        return stats

    def _mock_series_state(self, title, status):
        def closed_mock(self, original=Series._closed, title=title, status=status):
            if self.subject == title:
                return status
            else:
                return original(self)

        self.series_state_patcher = patch(
            "patchwork.Series._closed", side_effect=closed_mock, autospec=True
        )

    def _mock_series_expired(self, title, status):
        def expired_mock(self, original=Series._expired, title=title, status=status):
            if self.subject == title:
                return status
            else:
                return original(self)

        self.series_expired_patcher = patch(
            "patchwork.Series._expired", side_effect=expired_mock, autospec=True
        )

    def _mock_series_version(self, title, version):
        def version_mock(self, original=Series._expired, title=title, version=version):
            if self.subject == title:
                return version
            else:
                return original(self)

        def mock_relevant_series_cnt(
            self,
            subject,
            original=GithubSync._subject_count_series,
            title=title,
            cnt=version,
        ):
            if subject.subject == title:
                return version
            else:
                return original(self, subject)

        self.version_patcher = patch(
            "patchwork.Series._version", side_effect=version_mock, autospec=True
        )
        self.series_cnt_patcher = patch(
            "sources.github_sync.GithubSync._subject_count_series",
            side_effect=mock_relevant_series_cnt,
            autospec=True,
        )

    def test_create_pr(self):
        """
            - New PR has to be created if it doesn't exist
        """
        baseline = self.get_baseline()
        open_prs = self.gh_pulls_mock(state="open", base=TEST_BASE_BRANCH)
        self.gh_prs.remove(open_prs[0])
        self.worker.sync_branches()
        stats = self._get_current_stats()
        self.assertEqual(len(stats["created_prs"]) - len(baseline["created_prs"]), 1)
        self.assertEqual(stats["commented_prs"], baseline["commented_prs"])
        self.assertEqual(stats["closed_prs"], baseline["closed_prs"])
        self.assertEqual(stats["deleted_branches"], baseline["deleted_branches"])
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def test_comment_pr(self):
        """
            - Every time we have a diff for existing PR we must add a comment
        """
        baseline = self.get_baseline()

        # all diffs but first must return ""
        diffs = ["Dummy diff content"]
        for d in self.gh_prs:
            diffs.append("")
        self.git_diff_mock.side_effect = diffs
        self.worker.sync_branches()
        stats = self._get_current_stats()
        self.assertEqual(
            len(stats["commented_prs"]) - len(baseline["commented_prs"]), 1
        )
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(stats["closed_prs"], baseline["closed_prs"])
        self.assertEqual(stats["deleted_branches"], baseline["deleted_branches"])
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def test_close_pr(self):
        """
            - PR must be closed when series closed
            - branch must be deleted
        """
        baseline = self.get_baseline()
        open_prs = self.gh_pulls_mock(state="open", base=TEST_BASE_BRANCH)
        self._mock_series_state(open_prs[0]["title"], "closed")
        self.series_state_patcher.start()
        self.worker.sync_branches()
        self.series_state_patcher.stop()
        stats = self._get_current_stats()
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(len(stats["closed_prs"]) - len(baseline["closed_prs"]), 1)
        self.assertEqual(
            len(stats["commented_prs"]) - len(baseline["commented_prs"]), 1
        )
        self.assertEqual(stats["deleted_branches"] - baseline["deleted_branches"], 1)
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def test_expire_pr(self):
        """
            - PR must be closed when series expired
            - branch must remain untouched as long as series within TTL
        """
        baseline = self.get_baseline()
        open_prs = self.gh_pulls_mock(state="open", base=TEST_BASE_BRANCH)
        self._mock_series_expired(open_prs[0]["title"], True)
        self.series_expired_patcher.start()
        self.worker.sync_branches()
        self.series_expired_patcher.stop()
        stats = self._get_current_stats()
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(len(stats["closed_prs"]) - len(baseline["closed_prs"]), 1)
        self.assertEqual(
            len(stats["commented_prs"]) - len(baseline["commented_prs"]), 1
        )
        self.assertEqual(stats["deleted_branches"], baseline["deleted_branches"])
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def _delete_expired_branch(self, delta):
        # it's possible to have multiple PRs for same series due to previous implementation
        # we want uniq one for this test
        known_branches = [x.name for x in self.gh_get_branches()]
        found = False
        for pr in self.gh_pulls_mock(state="closed", base=TEST_BASE_BRANCH):
            if pr.head.ref not in known_branches:
                found = True
                break
        if not found:
            self.skipTest(
                "Fixtures doesn't contain uniq closed PR to test this scenario"
            )
        pr.updated_at.timestamp.return_value = FREEZE_TIME - delta  # -1m
        self.extra_branches.append(pr.head.ref)
        self.worker.sync_branches()
        return self._get_current_stats()

    def test_delete_expired_branch(self):
        """
            - branch must be deleted if related PR outside of TTL
        """
        baseline = self.get_baseline()
        stats = self._delete_expired_branch(60 * 60 * 24 * 30)
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(stats["closed_prs"], baseline["closed_prs"])
        self.assertEqual(stats["commented_prs"], baseline["commented_prs"])
        self.assertEqual(stats["deleted_branches"] - baseline["deleted_branches"], 1)
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def test_keep_expired_branch(self):
        """
            - branch must be kept in case it's within TTL
        """
        baseline = self.get_baseline()
        stats = self._delete_expired_branch(60 * 60)
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(stats["closed_prs"], baseline["closed_prs"])
        self.assertEqual(stats["commented_prs"], baseline["commented_prs"])
        self.assertEqual(stats["deleted_branches"], baseline["deleted_branches"])
        self.assertEqual(stats["reopened_prs"], baseline["reopened_prs"])

    def test_reopen_pr(self):
        """
            - PR has to be re-opened when branch exist
            - PR must be re-opened only when it's not the first version
            - PR must be re-opened if it has > 1 series in a subj
        """
        baseline = self.get_baseline()
        # Mock PR to pretend to be closed
        open_pr = self.gh_pulls_mock(state="open", base=TEST_BASE_BRANCH)[0]
        open_pr.state = "closed"
        # Mock branch to pretent to exist
        self.extra_branches.append(open_pr.head.ref)
        # Mock version and series in subject to pretent to be 2
        self._mock_series_version(open_pr.title, 2)
        self.version_patcher.start()
        self.series_cnt_patcher.start()
        # run cycle
        self.worker.sync_branches()
        # stop mock
        self.version_patcher.stop()
        self.series_cnt_patcher.stop()
        stats = self._get_current_stats()
        self.assertEqual(stats["created_prs"], baseline["created_prs"])
        self.assertEqual(
            len(stats["commented_prs"]) - len(baseline["commented_prs"]), 1
        )
        self.assertEqual(stats["closed_prs"], baseline["closed_prs"])
        self.assertEqual(stats["deleted_branches"], baseline["deleted_branches"])
        self.assertEqual(len(stats["reopened_prs"]) - len(baseline["reopened_prs"]), 1)


if __name__ == "__main__":
    unittest.main()
