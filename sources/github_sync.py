#!/usr/bin/env python3
from patchwork import Patchwork, Series, Subject
from github import Github, GithubException
import git
import re
import shutil
import os
import logging
import hashlib
import copy
import time

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class GithubSync(object):
    DEFAULT_STAT_VALUES = {
        "prs_created": (0, "Only new PRs, including merge-conflicts"),
        "prs_updated": (0, "PRs which exist before and which got a forcepush"),
        "prs_closed_total": (0, "All PRs that was closed"),
        "prs_closed_expired_reason": (0, "All PRs that was closed as expired"),
        "branches_deleted": (0, "Branches that was deleted"),
        "full_cycle_duration": (0, "Duration of one sync cycle"),
        "mirror_duration": (0, "Duration of mirroring upstream"),
        "pw_fetch_duration": (
            0,
            "Duration of initial search in PW, exclusing time to map existing PRs to PW entities",
        ),
        "patch_and_update_duration": (0, "Duration of git apply and git push"),
        "pw_to_git_latency": (
            0,
            "Average latency between patch created in PW and appear in GH",
        ),
        "full_clones": (
            0,
            "Number of times we had to do a full clone instead of git fetch",
        ),
        "partial_clones": (0, "Number of git fetches"),
        "merge_conflicts_total": (0, "All merge-conflicts"),
        "initial_merge_conflicts": (
            0,
            "Merge conflicts that happen for a first patch in a series known to us",
        ),
        "existing_pr_merge_conflicts": (
            0,
            "Merge conflicts on PRs that was fast-forwardable before",
        ),
        "all_known_subjects": (
            0,
            "All known subjects, including ones that was found in PW, GH. Also includes expired patches.",
        ),
        "prs_total": (0, "All prs within the scope of work for this worker"),
    }

    def __init__(
        self,
        pw_url,
        pw_search_patterns,
        master,
        repo_url,
        github_oauth_token,
        sync_from,
        source_master,
        ci_repo=None,
        ci_branch=None,
        merge_conflict_label="merge-conflict",
        pw_lookback=7,
        filter_tags=None,
    ):
        self.downstream_branch = "downstream"
        self.ci_repo = ci_repo
        if self.ci_repo:
            self.ci_branch = ci_branch
            self.ci_repo_dir = self._uniq_tmp_folder(ci_repo, ci_branch)
        self.repo_name = os.path.basename(repo_url)
        self.repo_url = repo_url
        self.sync_from = sync_from
        self.source_remote = "sync_from"
        self.master = master
        self.source_master = source_master
        self.git = Github(github_oauth_token)
        self.pw = Patchwork(
            pw_url, pw_search_patterns, pw_lookback=pw_lookback, filter_tags=filter_tags
        )
        self.user = self.git.get_user()
        self.user_login = self.user.login
        try:
            self.repo = self.user.get_repo(self.repo_name)
        except GithubException:
            # are we working under org repo?
            org = os.path.split(repo_url)[0].split(":")[-1]
            self.user_login = org
            self.repo = self.git.get_organization(org).get_repo(self.repo_name)

        self.branches = [x for x in self.repo.get_branches()]
        self.merge_conflict_label = merge_conflict_label
        # self.master = self.repo.get_branch(master)
        self.logger = logging.getLogger(__name__)
        self.repodir = self._uniq_tmp_folder(repo_url, master)

    def _uniq_tmp_folder(self, url, branch):
        # use same foder for multiple invocation to avoid cloning whole tree every time
        # but use different folder for different workers identified by url and branch name
        sha = hashlib.sha256()
        sha.update(f"{url}/{branch}".encode("utf-8"))
        return f"/tmp/pw_sync_{sha.hexdigest()}"

    def stat_update(self, key, increment=1):
        try:
            self.stats[key] += increment
        except:
            self.logger.error(f"Failed to update stats key: {key}, {increment}")

    def do_sync(self):
        # fetch upsteam and push to downstream
        if self.source_remote in self.local_repo.remotes:
            urls = list(self.local_repo.remote(self.source_remote).urls)
            if urls != [self.sync_from]:
                self.logger.warn(f"remote sync_from set to track {urls}, re-creating")
                self.local_repo.delete_remote(self.source_remote)
                self.local_repo.create_remote(self.source_remote, self.sync_from)
        else:
            self.local_repo.create_remote(self.source_remote, self.sync_from)
        self.source = self.local_repo.remote(self.source_remote)
        self.source.fetch(self.source_master)
        self.source_branch = getattr(self.source.refs, self.source_master)
        self._reset_repo(self.local_repo, f"{self.source_remote}/{self.source_master}")
        self.local_repo.git.push(
            "-f", "origin", f"{self.source_branch}:refs/heads/{self.master}"
        )
        self.master_sha = self.source_branch.object.hexsha

    def fetch_repo(self, path, url, branch):
        def full_sync(path, url, branch):
            shutil.rmtree(path, ignore_errors=True)
            r = git.Repo.clone_from(url, path)
            self._reset_repo(r, f"origin/{branch}")
            self.stat_update("full_clones")
            return r

        if os.path.exists(f"{path}/.git"):
            repo = git.Repo.init(path)
            try:
                repo.git.fetch("-p", "origin")
                self._reset_repo(repo, f"origin/{branch}")
                self.stat_update("partial_clones")
            except git.exc.GitCommandError as e:
                # fall back to a full sync
                repo = full_sync(path, url, branch)
        else:
            repo = full_sync(path, url, branch)
        return repo

    def fetch_master(self):
        """
            Fetch master only once
        """
        self.local_repo = self.fetch_repo(self.repodir, self.repo_url, self.master)
        self.ci_local_repo = self.fetch_repo(
            self.ci_repo_dir, self.ci_repo, self.ci_branch
        )
        self.ci_local_repo.git.checkout(f"origin/{self.ci_branch}")

    def _reset_repo(self, repo, branch):
        # wipe leftovers from previous cycle if any
        try:
            repo.git.am("--abort")
        except git.exc.GitCommandError:
            pass

        repo.git.reset("--hard", branch)
        repo.git.checkout(branch)

    def _create_dummy_commit(self, branch_name):
        """
            Reset branch, create dummy commit
        """
        self._reset_repo(self.local_repo, f"{self.source_remote}/{self.source_master}")
        if branch_name in self.local_repo.branches:
            self.local_repo.git.branch("-D", branch_name)
        self.local_repo.git.checkout("-b", branch_name)
        self.local_repo.git.commit("--allow-empty", "-m", "Dummy commit")
        self.local_repo.git.push("-f", "origin", branch_name)

    def _unflag_pr(self, pr):
        pr.remove_from_labels(self.merge_conflict_label)

    def _is_pr_flagged(self, pr):
        for label in pr.get_labels():
            if self.merge_conflict_label == label.name:
                return True
        return False

    def _close_pr(self, pr):
        pr.edit(state="closed")

    def _sync_pr_tags(self, pr, tags):
        pr.set_labels(*tags)
        for tag in tags:
            pr.add_to_labels(tag)

    def _comment_series_pr(
        self,
        series,
        message=None,
        branch_name=None,
        can_create=False,
        close=False,
        flag=False,
    ):
        """
            Appends comment to a PR.
        """
        title = f"{series.subject}"
        pr_tags = copy.copy(series.visible_tags)
        pr_tags.add(self.master)

        if flag:
            pr_tags.add(self.merge_conflict_label)

        if title in self.prs:
            pr = self.prs[title]
        elif can_create:
            self.logger.info(
                f"Creating PR for '{series.subject}' with {series.age} delay"
            )
            self.stat_update("pw_to_git_latency", series.age)
            self.stat_update("prs_created")
            if flag:
                self.stat_update("initial_merge_conflicts")
                self._create_dummy_commit(branch_name)
            body = (
                f"Pull request for series with\nsubject: {series.subject}\n"
                f"version: {series.version}\n"
                f"url: {series.web_url}\n"
            )
            pr = self.repo.create_pull(
                title=title, body=body, head=branch_name, base=self.master
            )
            self.prs[title] = pr
        else:
            return False

        if pr.state == "closed" and close:
            # If PR already closed do nothing
            return pr

        if (not flag) or (flag and not self._is_pr_flagged(pr)):
            if message:
                self.stat_update("prs_updated")
                pr.create_issue_comment(message)

        self._sync_pr_tags(pr, pr_tags)

        if close:
            self.stat_update("prs_closed_total")
            if series.expired:
                self.stat_update("prs_closed_expired_reason")
            self.logger.warning(f"Closing PR {pr}")
            self._close_pr(pr)
        return pr

    def checkout_and_patch(self, branch_name, series_to_apply):
        """
            Patch in place and push.
            Returns true if whole series applied.
            Return False if at least one patch in series failed.
            If at least one patch in series failed nothing gets pushed.
        """
        self.stat_update("all_known_subjects")
        self._reset_repo(self.local_repo, f"{self.source_remote}/{self.source_master}")
        if branch_name in self.local_repo.branches:
            self.local_repo.git.branch("-D", branch_name)
        self.local_repo.git.checkout("-b", branch_name)
        if series_to_apply.closed or series_to_apply.expired:
            if series_to_apply.closed:
                comment = f"At least one diff in series {series_to_apply.web_url} irrelevant now. Closing PR."
            else:
                comment = f"At least one diff in series {series_to_apply.web_url} expired. Closing PR."
                self.logger.warning(comment)
            self._comment_series_pr(series_to_apply, message=comment, close=True)
            # delete branch if there is no more PRs left from this branch
            if (
                branch_name in self.all_prs
                and len(self.all_prs[branch_name]) == 1
                and branch_name in self.branches
            ):
                self.logger.warning(f"Removing branch {branch_name}")
                self.stat_update("branches_deleted")
                self.repo.get_git_ref(f"heads/{branch_name}").delete()
            return False
        fname = None
        fname_commit = None
        title = f"{series_to_apply.subject}"
        comment = (
            f"Master branch: {self.master_sha}\nseries: {series_to_apply.web_url}\n"
            f"version: {series_to_apply.version}\n"
        )
        # TODO: omg this is damn ugly
        if self.ci_repo:
            os.system(
                f"cp -a {self.ci_repo_dir}/* {self.ci_repo_dir}/.travis.yml {self.repodir}"
            )
            self.local_repo.git.add("-A")
            self.local_repo.git.add("-f", ".travis.yml")
            self.local_repo.git.commit("-a", "-m", "adding ci files")

        fh = series_to_apply.patch_blob
        try:
            self.local_repo.git.am("-3", istream=fh)
        except git.exc.GitCommandError as e:
            conflict = self.local_repo.git.diff()
            comment = (
                f"{comment}\nPull request is *NOT* updated. Failed to apply {series_to_apply.web_url}\n"
                f"error message:\n```\n{e}\n```\n\n"
                f"conflict:\n```\n{conflict}\n```\n"
            )
            self.logger.warn(comment)
            self.stat_update("merge_conflicts_total")
            self._comment_series_pr(
                series_to_apply,
                message=comment,
                can_create=True,
                branch_name=branch_name,
                flag=True,
            )
            return False

        # force push only if if's a new branch or there is code diffs between old and new branches
        # which could mean that we applied new set of patches or just rebased
        if (
            branch_name not in self.local_repo.remotes.origin.refs
            or self.local_repo.git.diff(branch_name, f"remotes/origin/{branch_name}")
        ):
            self.local_repo.git.push("-f", "origin", branch_name)
            self._comment_series_pr(
                series_to_apply,
                message=comment,
                can_create=True,
                branch_name=branch_name,
            )
        else:
            # no code changes, just update tags
            self._comment_series_pr(series_to_apply)
        return True

    def get_pulls(self):
        for pr in self.repo.get_pulls():
            if self._is_relevant_pr(pr):
                self.prs[pr.title] = pr

            self.all_prs.setdefault(pr.head.ref, {}).setdefault(pr.base.ref, [])
            if pr.state == "open":
                self.all_prs[pr.head.ref][pr.base.ref].append(pr)

    def _is_relevant_pr(self, pr):
        """
            PR is relevant if it
            - coming from user
            - to same user
            - to branch {master}
            - is open
        """
        src_user = pr.head.user.login
        src_branch = pr.head.ref
        tgt_user = pr.base.user.login
        tgt_branch = pr.base.ref
        state = pr.state
        if (
            src_user == self.user_login
            and tgt_user == self.user_login
            and tgt_branch == self.master
            and state == "open"
        ):
            return True
        return False

    def _reset_stats(self):
        self.stats = {}
        for k in GithubSync.DEFAULT_STAT_VALUES:
            self.stats[k] = GithubSync.DEFAULT_STAT_VALUES[k][0]

    def sync_branches(self):
        """
            One subject = one branch
            creates branches when necessary
            apply patches where it's necessary
            delete branches where it's necessary
            version of series applies in the same branch
            as separate commit
        """

        # sync mirror and fetch current states of PRs
        self._reset_stats()
        self.pw.drop_counters()
        sync_start = time.time()
        self.subjects = {}
        self.prs = {}
        self.all_prs = {}
        self.fetch_master()
        self.get_pulls()
        self.do_sync()
        mirror_done = time.time()

        self.subjects = self.pw.get_relevant_subjects()
        pw_done = time.time()
        # fetch recent subjects
        for subject in self.subjects:
            series_id = subject.id
            # branch name == sid of the first known series
            branch_name = f"{subject.branch}=>{self.master}"
            # series to apply - last known series
            series = subject.latest_series
            self.checkout_and_patch(branch_name, series)
        # sync old subjects
        subject_names = [x.subject for x in self.subjects]
        for subject_name in self.prs:
            pr = self.prs[subject_name]
            if subject_name not in subject_names and self._is_relevant_pr(pr):
                branch_name = self.prs[subject_name].head.label.split(":")[1]
                series_id = branch_name.split("/")[1].split("=>")[0]
                series = Series(self.pw.get("series", series_id), self.pw)
                subject = Subject(series.subject, self.pw)
                branch_name = f"{subject.branch}=>{self.master}"
                self.checkout_and_patch(branch_name, subject.latest_series)
        patches_done = time.time()
        self.stat_update("full_cycle_duration", patches_done - sync_start)
        self.stat_update("mirror_duration", mirror_done - sync_start)
        self.stat_update("pw_fetch_duration", pw_done - mirror_done)
        self.stat_update("patch_and_update_duration", patches_done - pw_done)
        for p in self.prs:
            pr = self.prs[p]
            if self._is_relevant_pr(pr):
                self.stat_update("prs_total")
        if self.stats["prs_created"] > 0:
            self.stats["pw_to_git_latency"] = (
                self.stats["pw_to_git_latency"] / self.stats["prs_created"]
            )

        self.stats.update(self.pw.stats)
