#!/usr/bin/env python3
import json
from github_sync import GithubSync
from github import Github, GithubException
from subprocess import Popen, PIPE, STDOUT

import os
import time
import sys
import re
import argparse


def get_repo(git, project):
    repo_name = os.path.basename(project)
    try:
        user = git.get_user()
        repo = user.get_repo(repo_name)
    except GithubException:
        org = os.path.split(project)[0].split(":")[-1]
        repo = git.get_organization(org).get_repo(repo_name)
    return repo


class PWDaemon(object):
    def __init__(self, cfg, labels_cfg=None, logger=None):

        with open(cfg) as f:
            self.config = json.load(f)
        if labels_cfg:
            with open(labels_cfg) as f:
                self.labels_cfg = json.load(f)
        self.workers = []
        self.logger = logger
        for project in self.config.keys():
            for branch in self.config[project]["branches"].keys():
                worker_cfg = self.config[project]["branches"][branch]
                worker = GithubSync(
                    pw_url=worker_cfg["pw_url"],
                    pw_search_patterns=worker_cfg["pw_search_patterns"],
                    pw_lookback=worker_cfg.get("pw_lookback", 7),
                    master=branch,
                    repo_url=project,
                    github_oauth_token=self.config[project]["github_oauth_token"],
                    sync_from=worker_cfg["upstream"],
                    source_master=worker_cfg.get("upstream_branch", "master"),
                    ci_repo=worker_cfg.get("ci_repo", None),
                    ci_branch=worker_cfg.get("ci_branch", None),
                    filter_tags=worker_cfg.get("filter_tags", None),
                )
                self.workers.append(worker)
                if labels_cfg:
                    git = Github(self.config[project]["github_oauth_token"])
                    repo = get_repo(git, project)
                    self.color_labels(repo)

    def color_labels(self, repo):
        all_labels = {x.name.lower(): x for x in repo.get_labels()}
        for l in self.labels_cfg:
            label = l.lower()
            if label in all_labels:
                if (
                    all_labels[label].name != l
                    or all_labels[label].color != self.labels_cfg[l]
                ):
                    all_labels[label].edit(name=l, color=self.labels_cfg[l])
            else:
                repo.create_label(name=l, color=self.labels_cfg[l])

    def process_stats(self, worker, logger=None):
        if logger and os.path.isfile(logger) and os.access(logger, os.X_OK):
            metrics = {
                "int": {"time": time.time(),},
                "float": {},
                "normal": {"project": worker.repo_url, "branch": worker.master,},
            }
            for s in worker.stats:
                metrics["float"][s] = worker.stats[s]

            worker.repo_url
            p = Popen([logger], stdout=PIPE, stdin=PIPE, stderr=PIPE)
            stdout_data = p.communicate(input=json.dumps(metrics).encode())

    def loop(self):
        while True:
            for worker in self.workers:
                worker.sync_branches()
                self.process_stats(worker, self.logger)
            time.sleep(300)


def purge(cfg):
    with open(cfg) as f:
        config = json.load(f)
    for project in config.keys():
        git = Github(config[project]["github_oauth_token"])
        repo = get_repo(git, project)
        branches = [x for x in repo.get_branches()]
        for branch_name in branches:
            if re.match(r"series/[0-9]+.*", branch_name.name):
                repo.get_git_ref(f"heads/{branch_name.name}").delete()


def parse_args():
    parser = argparse.ArgumentParser(description="Starts kernel-patches daemon")
    parser.add_argument(
        "--config",
        default="~/.kernel-patches/config.json",
        help="Specify config location",
    )
    parser.add_argument(
        "--label-colors",
        default="~/.kernel-patches/labels.json",
        help="Specify label coloring config location.",
    )
    parser.add_argument(
        "--metric-logger",
        default="~/.kernel-patches/logger.sh",
        help="Specify external scripts which stdin will be fed with metrics",
    )
    parser.add_argument(
        "--action",
        default="start",
        choices=["start", "purge"],
        help="Purge will kill all existing PRs and delete all branches",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_args()
    cfg = os.path.expanduser(args.config)
    labels = os.path.expanduser(args.label_colors)
    logger = os.path.expanduser(args.metric_logger)
    if args.action == "purge":
        purge(cfg=cfg)
    else:
        d = PWDaemon(cfg=cfg, labels_cfg=labels, logger=logger)
        d.loop()
