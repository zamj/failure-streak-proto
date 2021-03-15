from __future__ import annotations
from typing import Optional, List, Dict, NamedTuple

import click
from evergreen import RetryingEvergreenApi, Version, Build, Tst
from evergreen.task import (
    Task,
)
from pydantic.main import BaseModel
from pymongo import MongoClient
from pymongo.database import Collection

PROJECT = "mongodb-mongo-master"
MONGO_URI = "localhost:27017"
DB_NAME = "test-status"
COLLECTION_NAME = "failure_streaks"
TASK_FAILURE_STATUS = "failed"


class Commit(BaseModel):
    version_id: str
    order: int


class FailureStreak(BaseModel):
    project: str
    build_variant: str
    task: str
    test: str
    beginning_commit: Optional[Commit]
    ending_commit: Optional[Commit]
    failing_versions: List[Commit]

    @classmethod
    def create_new(
        cls,
        project: str,
        task: str,
        test: str,
        build_variant: str,
        failing_versions: List[Commit],
        beginning_commit: Optional[Commit] = None,
        ending_commit: Optional[Commit] = None,
    ) -> FailureStreak:
        return FailureStreak(
            project=project,
            build_variant=build_variant,
            task=task,
            test=test,
            ending_commit=ending_commit,
            beginning_commit=beginning_commit,
            failing_versions=failing_versions,
        )


class TestEndingCommitGrouping(NamedTuple):
    test: str
    ending_commit: Optional[Commit]
    beginning_commit: Optional[Commit]


def _get_mongo_collection() -> Collection:
    return MongoClient(MONGO_URI).get_database(DB_NAME)[COLLECTION_NAME]


def _is_build_variant_required(name: str) -> bool:
    return name.startswith("!")


def _task_failed(task: Task) -> bool:
    return task.status == TASK_FAILURE_STATUS


def _get_task_from_version(
    task_name: str, build_variant: str, version: Version
) -> Optional[Task]:
    tasks = version.build_by_variant(build_variant).get_tasks()
    task = [task for task in tasks if task.display_name == task_name]
    if len(task) != 1:
        return None
    return task[0]


def _find_test(test_name: str, tests: List[Tst]) -> Optional[Tst]:
    matching = [test for test in tests if test.test_file == test_name]
    if len(matching) == 1:
        return matching[0]
    return None


def check_for_and_find_new_streaks(
    task: Task,
    cached_versions: Dict[int, Version],
    failing_tests: List[str],
    project_id: str,
    version_before_current: Optional[Version],
) -> Optional[List[FailureStreak]]:
    task_one_ahead = _get_task_from_version(
        task.display_name,
        task.build_variant,
        cached_versions[task.order + 1],
    )
    if not task_one_ahead:
        return None
    task_two_ahead = _get_task_from_version(
        task.display_name,
        task.build_variant,
        cached_versions[task.order + 2],
    )
    if not task_two_ahead:
        return None
    if not _task_failed(task_one_ahead) or not _task_failed(task_two_ahead):
        return None
    tests_one_ahead = task_one_ahead.get_tests(status="fail")
    tests_two_ahead = task_two_ahead.get_tests(status="fail")
    failing_tests_groupings = []
    for test in failing_tests:
        test_one_ahead = _find_test(test, tests_one_ahead)
        test_two_ahead = _find_test(test, tests_two_ahead)
        if test_one_ahead and test_two_ahead:
            ending_commit = None
            if cached_versions.get(task.order + 3):
                task_three_ahead = _get_task_from_version(
                    task.display_name,
                    task.build_variant,
                    cached_versions[task.order + 3],
                )
                if task_three_ahead:
                    test_three_ahead = _find_test(
                        test, task_three_ahead.get_tests(status="pass")
                    )
                    if test_three_ahead:
                        ending_commit = Commit(
                            order=task.order, version_id=task.version_id
                        )
            beginning_commit = _check_for_beginning_commit(
                test, task, version_before_current
            )
            failing_tests_groupings.append(
                TestEndingCommitGrouping(
                    test=test,
                    ending_commit=ending_commit,
                    beginning_commit=beginning_commit,
                )
            )

    if len(failing_tests_groupings) == 0:
        return None
    streaks = []
    for grouping in failing_tests_groupings:
        streaks.append(
            FailureStreak.create_new(
                project=project_id,
                build_variant=task.build_variant,
                task=task.display_name,
                test=grouping.test,
                failing_versions=[
                    Commit(order=task.order, version_id=task.version_id),
                    Commit(
                        order=task_one_ahead.order, version_id=task_one_ahead.version_id
                    ),
                    Commit(
                        order=task_two_ahead.order, version_id=task_two_ahead.version_id
                    ),
                ],
                ending_commit=grouping.ending_commit,
                beginning_commit=grouping.beginning_commit,
            )
        )
    return streaks


def _can_add_to_existing_streak(
    collection: Collection, failing_test: Tst, task: Task, project_id: str
) -> bool:
    result = collection.find_one(
        {
            "project": project_id,
            "build_variant": task.build_variant,
            "task": task.display_name,
            "test": failing_test.test_file,
            "failing_versions.order": task.order + 1,
        }
    )
    return result is not None and len(result) != 0


def _add_test_to_existing_streak(
    collection: Collection,
    failing_test: Tst,
    task: Task,
    project_id: str,
    beginning_commit: Optional[Commit] = None,
):
    query = {
        "project": project_id,
        "build_variant": task.build_variant,
        "task": task.display_name,
        "test": failing_test.test_file,
        "failing_versions.order": task.order + 1,
    }
    new_commit = Commit(order=task.order, version_id=task.version_id).dict()
    update = {
        "failing_versions": {
            "$cond": [
                {"$isArray": "$failing_versions"},
                {"$concatArrays": ["$failing_versions", [new_commit]]},
                [new_commit],
            ]
        }
    }
    if beginning_commit:
        update["beginning_commit"] = beginning_commit.dict()
    collection.update_one(
        query,
        [{"$set": update}],
    )


def try_to_add_to_existing_streaks(
    collection: Collection,
    failing_tests: List[Tst],
    task: Task,
    project_id: str,
    version_before_current: Optional[Version],
) -> List[str]:
    non_matching_tests = []
    for test in failing_tests:
        streak_exists = _can_add_to_existing_streak(collection, test, task, project_id)
        if streak_exists:
            print(f"Can add {task.task_id} {test.test_file} to an existing streak")
            commit = _check_for_beginning_commit(
                test.test_file, task, version_before_current
            )
            _add_test_to_existing_streak(collection, test, task, project_id, commit)
        else:
            non_matching_tests.append(test.test_file)
    return non_matching_tests


def _check_for_beginning_commit(
    test_name: str, task: Task, version_to_check: Optional[Version]
) -> Optional[Commit]:
    if not version_to_check:
        return None
    task_to_check = _get_task_from_version(
        task.display_name,
        task.build_variant,
        version_to_check,
    )
    if task_to_check:
        tests = task_to_check.get_tests(status="pass")
        test = _find_test(test_name, tests)
        if test:
            return Commit(
                order=task_to_check.order, version_id=task_to_check.version_id
            )
    return None


@click.command()
def main() -> None:
    """Find streaks of failures in required builders in Evergreen and store them in a mongodb db."""
    evg_client = RetryingEvergreenApi.get_api(use_config_file=True)
    collection = _get_mongo_collection()
    versions = evg_client.versions_by_project(PROJECT)
    cached_versions = {}
    for version in versions:
        # To find some data
        cached_versions[version.order] = version
        if version.order > 36940:
            continue
        print(version.order)
        if (
            not cached_versions.get(version.order + 1)
            or not cached_versions.get(version.order + 2)
            or not cached_versions.get(version.order + 3)
        ):
            continue
        version_before_current = version
        version = cached_versions.get(version.order + 1)
        builds = version.get_builds()
        for build in builds:
            if _is_build_variant_required(build.display_name):
                for task in build.get_tasks():
                    if _task_failed(task) or task.is_timeout():
                        failing_tests = task.get_tests(status="fail")
                        non_matching_tests = try_to_add_to_existing_streaks(
                            collection,
                            failing_tests,
                            task,
                            version.project,
                            version_before_current,
                        )
                        if len(non_matching_tests) > 0:
                            new_streaks = check_for_and_find_new_streaks(
                                task,
                                cached_versions,
                                non_matching_tests,
                                version.project,
                                version_before_current,
                            )
                            if not new_streaks:
                                continue
                            print(f"Found {len(new_streaks)} new streaks to add")
                            for streak in new_streaks:
                                collection.insert_one(
                                    streak.dict(exclude_none=True, exclude_unset=True)
                                )


if __name__ == "__main__":
    main()
