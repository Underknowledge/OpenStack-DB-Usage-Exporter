import os
import argparse
import pymysql
import functools
from datetime import datetime
from itertools import chain
import json
from collections import defaultdict

events_relevant_to_usage = [
    "create",
    "delete",
    "pause",
    "resume",
    "shelve",
    "start",
    "stop",
    "suspend",
    "unpause",
    "unshelve",
]

event_to_state_lookup = {
    "create": "active",
    "delete": "deleted",
    "resume": "active",
    "shelve": "shelved_offloaded",
    "start": "active",
    "stop": "stopped",
    "suspend": "suspended",
    "unshelve": "active",
}

state_charge_multipliers = {
    "active": 1,
    "deleted": 0,
    "error": 0,
    "not_yet_created": 0,
    "paused": 0.75,
    "resized": 1,
    "shelved_offloaded": 0,
    "stopped": 0.75,
    "suspended": 0.75,
}


def get_database_connection(db_name):
    host = os.environ.get("DB_HOST", "127.0.0.1")
    port = int(os.environ.get("DB_PORT", "3306"))
    user = os.environ.get("DB_USER", "root")
    password = os.environ.get("DB_PASSWORD", "root")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn


def get_reservations(
    cinder_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    sql = """
    SELECT created_at, project_id, resource, delta
    FROM reservations
    WHERE created_at BETWEEN %s AND %s
    """
    values = [start, end]
    if project_id:
        sql += " AND project_id = %s"
        values.append(project_id)
    if ignore_project_ids:
        placeholders = ", ".join(["%s"] * len(ignore_project_ids))
        sql += f" AND project_id NOT IN ({placeholders})"
        values.extend(ignore_project_ids)

    with cinder_db_conn.cursor() as cursor:
        cursor.execute(sql, values)
        result_rows = cursor.fetchall()

    # Grouping by created_at and project_id
    grouped_reservations = defaultdict(
        lambda: {"created_at": None, "project_id": None, "resources": []}
    )

    for row in result_rows:
        created_at = (
            row["created_at"].strftime("%Y-%m-%dT%H:%M:%S")
            if isinstance(row["created_at"], datetime)
            else row["created_at"]
        )
        key = (created_at, row["project_id"])

        # Initialize group if not already
        grouped_reservations[key]["created_at"] = created_at
        grouped_reservations[key]["project_id"] = row["project_id"]

        # Append resource and delta into resources list
        grouped_reservations[key]["resources"].append(
            {"resource": row["resource"], "delta": row["delta"]}
        )

    # Convert defaultdict to a list of dictionaries
    adapted_cinder_rows = list(grouped_reservations.values())

    return adapted_cinder_rows


def get_non_resize_events_for_instance(db_conn, instance_uuid):
    query_results = get_non_resize_events_for_instance_sql_query_(
        db_conn, instance_uuid
    )
    events = get_non_resize_events_for_instance_query_results_to_events_(query_results)
    return events


def get_non_resize_events_for_instance_sql_query_(db_conn, instance_uuid):
    sql = "SELECT created_at, action FROM instance_actions WHERE action IN %s AND instance_uuid=%s"
    with db_conn.cursor() as cursor:
        cursor.execute(sql, (events_relevant_to_usage, instance_uuid))
        return cursor.fetchall()


def get_non_resize_events_for_instance_query_results_to_events_(results):
    def result_to_event_dict(result):
        # Returns an event in a dict format like:
        # {'timestamp': datetime.datetime(2022, 2, 14, 16, 41, 2), 'event': 'create'}
        return {
            "timestamp": result.get("created_at"),
            "event": result.get("action"),
        }

    return list(map(result_to_event_dict, results))


def get_flavor_history_for_instance(db_conn, instance_uuid):
    # Returns a dict with keys:
    # - initial_flavor_id with value containing initial flavor ID
    # - resize_events with value containing a list of resize events

    def migration_result_to_dict(result):
        return {
            "created_timestamp": result.get("created_at"),
            "updated_timestamp": result.get("updated_at"),
            "status": result.get("status"),
            "old_flavor_id": result.get("old_instance_type_id"),
            "new_flavor_id": result.get("new_instance_type_id"),
        }

    sql = "SELECT created_at, updated_at, status, old_instance_type_id, new_instance_type_id FROM migrations WHERE migration_type = 'resize' AND instance_uuid=%s"
    with db_conn.cursor() as cursor:
        cursor.execute(sql, instance_uuid)
        results = cursor.fetchall()
    migrations = map(migration_result_to_dict, results)

    def migration_to_resize_events(migration):
        if migration.get("status") in ["finished", "confirmed"]:
            return [
                {
                    "timestamp": migration.get("created_timestamp"),
                    "event": "resize",
                    "old_flavor_id": migration.get("old_flavor_id"),
                    "new_flavor_id": migration.get("new_flavor_id"),
                }
            ]
        elif migration.get("status") == "reverted":
            # Treat a reverted migration as two resize events
            return [
                # The first happens when the migration is initiated
                {
                    "timestamp": migration.get("created_timestamp"),
                    "event": "resize",
                    "old_flavor_id": migration.get("old_flavor_id"),
                    "new_flavor_id": migration.get("new_flavor_id"),
                },
                # The second happens when the migration is reverted
                {
                    "timestamp": migration.get("updated_timestamp"),
                    "event": "resize",
                    "old_flavor_id": migration.get("new_flavor_id"),
                    "new_flavor_id": migration.get("old_flavor_id"),
                },
            ]
        elif migration.get("status") in ["pre-migrating", "error"]:
            # not considered a resize for billing purposes
            return []
        else:
            raise Exception(
                "This code doesn't know how to handle migration status '"
                + migration.get("status")
                + "'"
            )

    resize_events_from_migrations = map(migration_to_resize_events, migrations)
    resize_events_flat_list = list(chain.from_iterable(resize_events_from_migrations))

    def initial_flavor_id():
        try:
            first_resize_event = resize_events_flat_list[0]
            return first_resize_event.get("old_flavor_id")
        except IndexError:
            # No resize events, so the current flavor _is_ the initial flavor
            sql = "SELECT instance_type_id FROM instances WHERE uuid=%s"
            with db_conn.cursor() as cursor:
                cursor.execute(sql, instance_uuid)
                try:
                    flavor_id = cursor.fetchone().get("instance_type_id")
                except AttributeError as e:
                    raise Exception(
                        "Could not find flavor ID for instance. Instance UUID may be invalid."
                    )
                return flavor_id

    return {
        "initial_flavor_id": initial_flavor_id(),
        "resize_events": resize_events_flat_list,
    }


def instance_create_time(nova_db_conn, instance_uuid):
    """Returns the time that an instance was created."""
    sql = "SELECT created_at from instances WHERE uuid = %s"
    with nova_db_conn.cursor() as cursor:
        cursor.execute(sql, instance_uuid)
        result = cursor.fetchone()
    return result.get("created_at")


def get_synthetic_create_event(nova_db_conn, instance_uuid):
    """Returns a create event for an instance, even if a create event does not exist in the instance_actions table."""
    create_time = instance_create_time(nova_db_conn, instance_uuid)
    if create_time:
        event = {"timestamp": create_time, "event": "create"}
        return event
    else:
        raise Exception("This instance has no creation time in the Nova database")


def instance_delete_time(nova_db_conn, instance_uuid):
    """Returns the time that an instance was deleted, if it is deleted. If the instance is not deleted, returns None."""
    sql = "SELECT deleted_at from instances WHERE uuid = %s"
    with nova_db_conn.cursor() as cursor:
        cursor.execute(sql, instance_uuid)
        result = cursor.fetchone()
    return result.get("deleted_at")


def get_synthetic_delete_event(nova_db_conn, instance_uuid):
    """Returns a delete event if a given instance is deleted, even if a delete event does not exist in the instance_actions table. Otherwise, returns None."""
    delete_time = instance_delete_time(nova_db_conn, instance_uuid)
    if delete_time:
        event = {"timestamp": delete_time, "event": "delete"}
        return event
    else:
        return None


def combined_events_for_instance(db_conn, instance_uuid, start, end):
    events = get_non_resize_events_for_instance(db_conn, instance_uuid)
    flavor_history_dict = get_flavor_history_for_instance(db_conn, instance_uuid)
    try:
        create_event = events[0]
    except IndexError:
        # Instance has no explicit create event, so make a synthetic one from create time in DB
        create_event = get_synthetic_create_event(db_conn, instance_uuid)
    create_event_with_flavor = {
        **create_event,
        **{"new_flavor_id": flavor_history_dict.get("initial_flavor_id")},
    }
    try:
        events_after_create = events[1:] + flavor_history_dict.get("resize_events")
    except IndexError:
        # Handling case of no further events
        events_after_create = flavor_history_dict.get("resize_events")

    # Look for a delete event in the list of events. If there isn't one, then make a synthetic delete event if the instance is deleted.
    has_delete_event = any(
        event.get("event") == "delete" for event in events_after_create
    )
    synthetic_delete_event = (
        get_synthetic_delete_event(db_conn, instance_uuid)
        if not has_delete_event
        else None
    )

    all_events = [create_event_with_flavor] + events_after_create
    if synthetic_delete_event:
        all_events.append(synthetic_delete_event)

    all_sorted_events = sorted(
        all_events,
        key=lambda e: e.get("timestamp"),
    )
    return all_sorted_events


def get_state_intervals_from_events(events):
    def process_next_state_interval(existing_intervals, next_event):
        # Given a list of state intervals (existing_intervals) and a next_event, return the new list of state intervals

        if len(existing_intervals) == 0:
            # First event will be create
            if next_event.get("event") == "create":
                return [
                    {
                        "start_timestamp": next_event.get("timestamp"),
                        "end_timestamp": None,
                        "state": "active",
                        "flavor_id": next_event.get("new_flavor_id"),
                    }
                ]
            else:
                raise Exception("List of events must begin with create")
        else:
            prev_interval = existing_intervals[-1]

            new_prev_interval = {
                "start_timestamp": prev_interval.get("start_timestamp"),
                "end_timestamp": next_event.get("timestamp"),
                "state": prev_interval.get("state"),
                "flavor_id": prev_interval.get("flavor_id"),
            }

            new_existing_intervals = existing_intervals[:-1] + [new_prev_interval]

            next_state = event_to_state_lookup.get(next_event.get("event"))

            # If next event doesn't define a new_flavor_id, keep using the old one
            next_interval_flavor_id = next_event.get(
                "new_flavor_id"
            ) or prev_interval.get("flavor_id")

            next_interval = {
                "start_timestamp": next_event.get("timestamp"),
                "end_timestamp": None,
                "state": (
                    next_state if next_state is not None else prev_interval.get("state")
                ),
                "flavor_id": next_interval_flavor_id,
            }

            new_intervals = new_existing_intervals + [next_interval]
            return new_intervals

    return list(functools.reduce(process_next_state_interval, events, []))


def get_flavor_info(nova_api_db_conn, flavor_id):
    # returns a dict with keys:
    # - name
    # - vcpus
    # - ram (MB)

    sql = "SELECT name, vcpus, memory_mb FROM flavors WHERE id = %s"
    with nova_api_db_conn.cursor() as cursor:
        cursor.execute(sql, flavor_id)
        flavor_result = cursor.fetchone()
    if flavor_result is None:
        raise Exception(f"Flavor ID {flavor_id} not found in the database.")
    flavor = {
        "name": flavor_result.get("name"),
        "vcpus": flavor_result.get("vcpus"),
        "ram": flavor_result.get("memory_mb"),
    }
    return flavor


def get_total_usage_for_instance(
    nova_db_conn, nova_api_db_conn, instance_uuid, start, end, debug=False
):
    # returns the total cpu_hours and ram_gib_hours for the instance
    # if debug is True, also returns detailed info

    events = combined_events_for_instance(nova_db_conn, instance_uuid, start, end)
    state_intervals = get_state_intervals_from_events(events)
    total_cpu_hours = 0
    total_ram_gib_hours = 0
    detailed_info = []

    for state_interval in state_intervals:
        # Adjust interval to be within the specified time range
        interval_start = max(state_interval.get("start_timestamp"), start)
        interval_end = min(state_interval.get("end_timestamp") or end, end)
        if interval_start >= interval_end:
            # Interval is outside the specified time range
            # Include in detailed_info with zero duration
            flavor_info = get_flavor_info(
                nova_api_db_conn, state_interval.get("flavor_id")
            )
            detailed_info.append(
                {
                    "start": interval_start.isoformat(),
                    "end": interval_end.isoformat(),
                    "duration_hours": 0,
                    "state": state_interval.get("state"),
                    "flavor_id": state_interval.get("flavor_id"),
                    "vcpus": flavor_info.get("vcpus"),
                    "ram_gib": flavor_info.get("ram") / 1024,
                    "cpu_hours": 0,
                    "ram_gib_hours": 0,
                    "in_time_range": False,
                }
            )
            continue

        duration = interval_end - interval_start
        duration_in_hours = duration.total_seconds() / 3600

        flavor_info = get_flavor_info(nova_api_db_conn, state_interval.get("flavor_id"))
        vcpus = flavor_info.get("vcpus")
        ram_gib = flavor_info.get("ram") / 1024  # Convert MB to GiB

        state = state_interval.get("state")
        multiplier = state_charge_multipliers.get(state, 1)

        cpu_hours = duration_in_hours * vcpus * multiplier
        ram_gib_hours = duration_in_hours * ram_gib * multiplier

        total_cpu_hours += cpu_hours
        total_ram_gib_hours += ram_gib_hours

        detailed_info.append(
            {
                "start": interval_start.isoformat(),
                "end": interval_end.isoformat(),
                "duration_hours": duration_in_hours,
                "state": state,
                "flavor_id": state_interval.get("flavor_id"),
                "vcpus": vcpus,
                "ram_gib": ram_gib,
                "multiplier": multiplier,
                "cpu_hours": cpu_hours,
                "ram_gib_hours": ram_gib_hours,
                "in_time_range": True,
            }
        )
    return total_cpu_hours, total_ram_gib_hours, detailed_info


def get_extant_instances(
    nova_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    sql = """SELECT uuid, created_at, deleted_at, vm_state, project_id, user_id FROM instances WHERE created_at <= %s AND (deleted_at IS NULL OR deleted_at >= %s)AND hidden = 0"""
    values = [end, start]

    if project_id:
        sql += " AND project_id = %s"
        values.append(project_id)

    if ignore_project_ids:
        placeholders = ", ".join(["%s"] * len(ignore_project_ids))
        sql += f" AND project_id NOT IN ({placeholders})"
        values.extend(ignore_project_ids)

    with nova_db_conn.cursor() as cursor:
        cursor.execute(sql, values)
        result_rows = cursor.fetchall()

    # Return all instances (filtered by project_id and ignore_project_ids if provided)
    return result_rows


def instance_is_outside_time_range(instance, start, end):
    created_at = instance.get("created_at")
    deleted_at = instance.get("deleted_at") or datetime.now()

    if deleted_at <= start or created_at >= end:
        return True
    else:
        return False


def calculate_usage_per_instance(
    nova_db_conn, nova_api_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    instances = get_extant_instances(
        nova_db_conn, start, end, project_id, ignore_project_ids
    )
    instance_usage = []

    for instance in instances:
        instance_uuid = instance.get("uuid")
        project_id = instance.get("project_id")
        user_id = instance.get("user_id")
        total_cpu_hours, total_ram_gib_hours, detailed_info = (
            get_total_usage_for_instance(
                nova_db_conn, nova_api_db_conn, instance_uuid, start, end
            )
        )
        # Collect additional info
        create_time = instance.get("created_at")
        delete_time = instance.get("deleted_at") or datetime.now()
        total_duration = (
            min(delete_time, end) - max(create_time, start)
        ).total_seconds() / 3600
        if total_duration < 0:
            total_duration = 0
        has_usage = total_cpu_hours > 0 or total_ram_gib_hours > 0
        instance_usage.append(
            {
                "instance_uuid": instance_uuid,
                "project_id": project_id,
                "user_id": user_id,
                "total_cpu_hours": total_cpu_hours,
                "total_ram_gib_hours": total_ram_gib_hours,
                "total_duration_hours": total_duration,
                "details": detailed_info,
                "has_usage": has_usage,
                "outside_time_range": not has_usage
                and instance_is_outside_time_range(instance, start, end),
            }
        )
    return instance_usage


def get_project_names(keystone_db_conn):
    sql = "SELECT id, name FROM project"
    with keystone_db_conn.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()
    project_names = {row["id"]: row["name"] for row in results}
    return project_names


def get_volume_type_names(cinder_db_conn):
    sql = "SELECT id, name FROM volume_types"
    with cinder_db_conn.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()
    volume_type_names = {row["id"]: row["name"] for row in results}
    return volume_type_names


# get volumes that existed during the time period
def get_extant_volumes(
    cinder_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    sql = """SELECT id, created_at, deleted_at, size, project_id, volume_type_id FROM volumes WHERE created_at <= %s AND (deleted_at IS NULL OR deleted_at >= %s)"""
    values = [end, start]
    if project_id:
        sql += " AND project_id = %s"
        values.append(project_id)
    if ignore_project_ids:
        placeholders = ", ".join(["%s"] * len(ignore_project_ids))
        sql += f" AND project_id NOT IN ({placeholders})"
        values.extend(ignore_project_ids)
    with cinder_db_conn.cursor() as cursor:
        cursor.execute(sql, values)
        result_rows = cursor.fetchall()
    return result_rows


def calculate_volume_usage_per_volume(volumes, start, end, volume_type_names):
    volume_usage = []
    for volume in volumes:
        volume_id = volume.get("id")
        project_id = volume.get("project_id")
        volume_type_id = volume.get("volume_type_id")
        volume_type_name = volume_type_names.get(volume_type_id, "Unknown Volume Type")
        size_gb = volume.get("size")
        create_time = volume.get("created_at")
        delete_time = volume.get("deleted_at") or end
        # Adjust times to within start and end
        interval_start = max(create_time, start)
        interval_end = min(delete_time, end)
        if interval_start >= interval_end:
            duration_hours = 0
            continue  # Skip volumes with no usage during the time range
        else:
            duration = interval_end - interval_start
            duration_hours = duration.total_seconds() / 3600
        gib_hours = duration_hours * size_gb  # Size is in GB
        volume_usage.append(
            {
                "volume_id": volume_id,
                "project_id": project_id,
                "volume_type_name": volume_type_name,
                "size_gb": size_gb,
                "gib_hours": gib_hours,
                "details": {
                    "interval_start": interval_start.isoformat(),
                    "interval_end": interval_end.isoformat(),
                    "duration_hours": duration_hours,
                },
            }
        )
    return volume_usage


def aggregate_volume_usage_by_project(volume_usage, ignore_project_ids=None):
    project_volume_usage = {}
    for usage in volume_usage:
        project_id = usage["project_id"]
        if ignore_project_ids and project_id in ignore_project_ids:
            continue
        if project_id not in project_volume_usage:
            project_volume_usage[project_id] = {"gib_hours_per_type": {}, "volumes": []}
        volume_type_name = usage["volume_type_name"]
        gib_hours = usage["gib_hours"]
        project_volume_usage[project_id]["gib_hours_per_type"].setdefault(
            volume_type_name, 0
        )
        project_volume_usage[project_id]["gib_hours_per_type"][
            volume_type_name
        ] += gib_hours
        project_volume_usage[project_id]["volumes"].append(usage)
    return project_volume_usage


def aggregate_usage_by_project(instance_usage, project_names, ignore_project_ids=None):
    project_usage = {}

    for usage in instance_usage:
        project_id = usage["project_id"]
        if ignore_project_ids and project_id in ignore_project_ids:
            continue  # Skip ignored projects
        project_name = project_names.get(project_id, "Unknown Project")
        if project_id not in project_usage:
            project_usage[project_id] = {
                "project_name": project_name,
                "cpu_hours": 0,
                "ram_gib_hours": 0,
                "machines": [],
                "volumes": [],
                "volume_gib_hours_per_type": {},
                "images": [],
                "image_gib_hours": 0,
                "snapshots": [],
                "snapshot_gib_hours": 0,
            }
        project_usage[project_id]["cpu_hours"] += usage["total_cpu_hours"]
        project_usage[project_id]["ram_gib_hours"] += usage["total_ram_gib_hours"]

        machine_info = {
            "instance_uuid": usage["instance_uuid"],
            "user_id": usage["user_id"],
            "total_cpu_hours": usage["total_cpu_hours"],
            "total_ram_gib_hours": usage["total_ram_gib_hours"],
            "details": usage["details"],
            "total_duration_hours": usage["total_duration_hours"],
            "outside_time_range": usage["outside_time_range"],
        }
        project_usage[project_id]["machines"].append(machine_info)

    return project_usage


# get images that existed during the time period
def get_extant_images(
    glance_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    sql = """SELECT id, name, size, status, deleted, created_at, updated_at, deleted_at, owner FROM images WHERE created_at <= %s AND (deleted_at IS NULL OR deleted_at >= %s OR updated_at >= %s)"""
    values = [end, start, start]
    if project_id:
        sql += " AND owner = %s"
        values.append(project_id)
    if ignore_project_ids:
        placeholders = ", ".join(["%s"] * len(ignore_project_ids))
        sql += f" AND owner NOT IN ({placeholders})"
        values.extend(ignore_project_ids)
    with glance_db_conn.cursor() as cursor:
        cursor.execute(sql, values)
        result_rows = cursor.fetchall()
    return result_rows


# calculate image usage per image
def calculate_image_usage_per_image(images, start, end):
    image_usage = []
    for image in images:
        image_id = image.get("id")
        project_id = image.get("owner")
        image_name = image.get("name")
        size_bytes = image.get("size") or 0  # Handle possible NULL sizes
        size_gb = size_bytes / (1024**3)  # Convert bytes to GiB
        create_time = image.get("created_at")
        # Determine delete time
        if image.get("status") == "deleted" or image.get("deleted") == 1:
            delete_time = image.get("deleted_at") or image.get("updated_at") or end
            image_deleted = True
        else:
            delete_time = end
            image_deleted = False
        # Adjust times to within start and end
        interval_start = max(create_time, start)
        interval_end = min(delete_time, end)
        if interval_start >= interval_end:
            duration_hours = 0
            continue  # Skip images with no usage during the time range
        else:
            duration = interval_end - interval_start
            duration_hours = duration.total_seconds() / 3600
        gib_hours = duration_hours * size_gb
        image_usage.append(
            {
                "image_id": image_id,
                "project_id": project_id,
                "image_name": image_name,
                "size_gb": size_gb,
                "gib_hours": gib_hours,
                "details": {
                    "interval_start": interval_start.isoformat(),
                    "interval_end": interval_end.isoformat(),
                    "duration_hours": duration_hours,
                    "image_created": create_time.isoformat(),
                    "image_deleted": (
                        delete_time.isoformat() if image_deleted else "none"
                    ),
                },
            }
        )
    return image_usage


# aggregate image usage by project
def aggregate_image_usage_by_project(image_usage, ignore_project_ids=None):
    project_image_usage = {}
    for usage in image_usage:
        project_id = usage["project_id"]
        if ignore_project_ids and project_id in ignore_project_ids:
            continue
        if project_id not in project_image_usage:
            project_image_usage[project_id] = {"gib_hours": 0, "images": []}
        gib_hours = usage["gib_hours"]
        project_image_usage[project_id]["gib_hours"] += gib_hours
        project_image_usage[project_id]["images"].append(usage)
    return project_image_usage


# get snapshots that existed during the time period
def get_extant_snapshots(
    cinder_db_conn, start, end, project_id=None, ignore_project_ids=None
):
    sql = """SELECT * FROM snapshots WHERE use_quota = 1 AND progress = %s AND created_at <= %s AND (deleted_at IS NULL OR deleted_at >= %s OR updated_at >= %s)"""
    values = ["100%", end, start, start]
    if project_id:
        sql += " AND project_id = %s"
        values.append(project_id)
    if ignore_project_ids:
        placeholders = ", ".join(["%s"] * len(ignore_project_ids))
        sql += f" AND project_id NOT IN ({placeholders})"
        values.extend(ignore_project_ids)
    with cinder_db_conn.cursor() as cursor:
        cursor.execute(sql, values)
        result_rows = cursor.fetchall()
    return result_rows


# calculate snapshot usage per snapshot
def calculate_snapshot_usage_per_snapshot(snapshots, start, end):
    snapshot_usage = []
    for snapshot in snapshots:
        snapshot_id = snapshot.get("id")
        project_id = snapshot.get("project_id")
        user_id = snapshot.get("user_id")
        volume_id = snapshot.get("volume_id")
        volume_size_gb = snapshot.get("volume_size")  # volume_size is int(11)

        create_time = snapshot.get("created_at")
        status = snapshot.get("status")
        updated_at = snapshot.get("updated_at")

        note = ""
        if status == "error_deleting":
            end_time = updated_at
            note = "Used updated_at as end_time because status is error_deleting."
        elif status == "deleted":
            end_time = snapshot.get("deleted_at") or end
        else:
            # For 'available' and other statuses
            end_time = end

        # Adjust times to within start and end
        interval_start = max(create_time, start)
        interval_end = min(end_time, end)
        if interval_start >= interval_end:
            continue  # Skip snapshots not overlapping with time range

        duration = interval_end - interval_start
        duration_hours = duration.total_seconds() / 3600
        gib_hours = duration_hours * volume_size_gb  # volume_size is in GB

        snapshot_info = {
            "snapshot_id": snapshot_id,
            "volume_id": volume_id,
            "user_id": user_id,
            "volume_size_gb": volume_size_gb,
            "project_id": project_id,
            "gib_hours": gib_hours,
            "details": {
                "interval_start": interval_start.isoformat(),
                "interval_end": interval_end.isoformat(),
                "duration_hours": duration_hours,
                "status": status,
                "create_time": create_time.isoformat(),
                "updated_at": updated_at.isoformat() if updated_at else None,
                "end_time": end_time.isoformat(),
                "note": note,
            },
        }
        snapshot_usage.append(snapshot_info)
    return snapshot_usage


# aggregate snapshot usage by project
def aggregate_snapshot_usage_by_project(snapshot_usage, ignore_project_ids=None):
    project_snapshot_usage = {}
    for usage in snapshot_usage:
        project_id = usage["project_id"]
        if ignore_project_ids and project_id in ignore_project_ids:
            continue
        if project_id not in project_snapshot_usage:
            project_snapshot_usage[project_id] = {"gib_hours": 0, "snapshots": []}
        gib_hours = usage["gib_hours"]
        project_snapshot_usage[project_id]["gib_hours"] += gib_hours
        project_snapshot_usage[project_id]["snapshots"].append(usage)
    return project_snapshot_usage


# check if Octavia quota is meaningful
def is_meaningful_octavia_quota(quota):
    quota_fields = [
        "load_balancer",
        "listener",
        "pool",
        "health_monitor",
        "member",
        "l7policy",
        "l7rule",
    ]
    for field in quota_fields:
        value = quota.get(field)
        if value is not None:
            return True
    return False


# get quotas for a list of project_ids from Octavia
def get_quotas_for_projects(octavia_db_conn, project_ids):
    if not project_ids:
        return []
    placeholders = ", ".join(["%s"] * len(project_ids))
    sql = f"SELECT * FROM quotas WHERE project_id IN ({placeholders})"
    with octavia_db_conn.cursor() as cursor:
        cursor.execute(sql, project_ids)
        results = cursor.fetchall()
    return results


# get load balancer details per project
def get_load_balancer_details_per_project(octavia_db_conn, project_id=None):
    sql = """
    SELECT lb.*, vip.ip_address as vip_address, vip.network_id as vip_network_id,
           vip.port_id as vip_port_id, vip.subnet_id as vip_subnet_id
    FROM load_balancer lb
    LEFT JOIN vip ON lb.id = vip.load_balancer_id
    WHERE lb.project_id = %s
    """
    with octavia_db_conn.cursor() as cursor:
        cursor.execute(sql, (project_id,))
        results = cursor.fetchall()
    load_balancers = []
    for row in results:
        lb_info = {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "provisioning_status": row["provisioning_status"],
            "operating_status": row["operating_status"],
            "enabled": row["enabled"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "provider": row.get("provider"),
            "topology": row.get("topology"),
            "server_group_id": row.get("server_group_id"),
            "flavor_id": row.get("flavor_id"),
            "availability_zone": row.get("availability_zone"),
            "vip_address": row.get("vip_address"),
            "vip_network_id": row.get("vip_network_id"),
            "vip_port_id": row.get("vip_port_id"),
            "vip_subnet_id": row.get("vip_subnet_id"),
            # Add 'listeners' next
        }
        # Now get listeners for this load balancer
        listeners = get_listeners_for_load_balancer(octavia_db_conn, lb_info["id"])
        lb_info["listeners"] = listeners
        load_balancers.append(lb_info)
    return load_balancers


def get_listeners_for_load_balancer(octavia_db_conn, load_balancer_id):
    sql = """
    SELECT l.*
    FROM listener l
    WHERE l.load_balancer_id = %s
    """
    with octavia_db_conn.cursor() as cursor:
        cursor.execute(sql, (load_balancer_id,))
        results = cursor.fetchall()
    listeners = []
    for row in results:
        listener_info = {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "protocol": row["protocol"],
            "protocol_port": row["protocol_port"],
            "connection_limit": row["connection_limit"],
            "load_balancer_id": row["load_balancer_id"],
            "provisioning_status": row["provisioning_status"],
            "operating_status": row["operating_status"],
            "enabled": row["enabled"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            # other fields?
            # Get statistics
        }
        # get statistics for this listener
        statistics = get_listener_statistics(octavia_db_conn, listener_info["id"])
        listener_info["statistics"] = statistics
        listeners.append(listener_info)
    return listeners


def get_listener_statistics(octavia_db_conn, listener_id):
    sql = """
    SELECT ls.*
    FROM listener_statistics ls
    WHERE ls.listener_id = %s
    """
    with octavia_db_conn.cursor() as cursor:
        cursor.execute(sql, (listener_id,))
        results = cursor.fetchall()
    statistics = []
    for row in results:
        stats_info = {
            "listener_id": row["listener_id"],
            "bytes_in": row["bytes_in"],
            "bytes_out": row["bytes_out"],
            "active_connections": row["active_connections"],
            "total_connections": row["total_connections"],
            "amphora_id": row["amphora_id"],
            "request_errors": row["request_errors"],
        }
        statistics.append(stats_info)
    return statistics


def main():
    parser = argparse.ArgumentParser(
        description="Calculate OpenStack usage per VM per project."
    )
    parser.add_argument(
        "--start-time", required=True, help="Start time in format YYYY-MM-DD HH:MM:SS"
    )
    parser.add_argument(
        "--end-time", required=True, help="End time in format YYYY-MM-DD HH:MM:SS"
    )
    parser.add_argument("--project-id", help="Optional project ID to filter on")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument(
        "--octavia", action="store_true", help="Include Octavia load balancer usage"
    )
    parser.add_argument(
        "--yes-i-know-that-i-can-not-query-historical-data",
        action="store_true",
        dest="acknowledge_no_historical_data",
        help="Confirm you know that historical data cannot be queried for volumes and Octavia",
    )
    parser.add_argument(
        "--exclude-project",
        action="append",
        help="Project ID or name to exclude from the calculation. Can be specified multiple times.",
    )

    args = parser.parse_args()

    start_time = datetime.strptime(args.start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(args.end_time, "%Y-%m-%d %H:%M:%S")
    project_id = args.project_id
    debug = args.debug

    # Service holds Octavia instances, these will get billed differently
    DEFAULT_IGNORE_PROJECTS = ["service"]
    exclude_projects = args.exclude_project or []
    ignore_project_names = list(set(DEFAULT_IGNORE_PROJECTS + exclude_projects))

    nova_db_conn = get_database_connection("nova")
    nova_api_db_conn = get_database_connection("nova_api")
    keystone_db_conn = get_database_connection("keystone")
    cinder_db_conn = get_database_connection("cinder")
    glance_db_conn = get_database_connection("glance")

    try:
        project_names = get_project_names(keystone_db_conn)
        project_ids_by_name = {v: k for k, v in project_names.items()}

        ignore_project_ids = set()
        for proj_name in ignore_project_names:
            if proj_name in project_ids_by_name:
                proj_id = project_ids_by_name[proj_name]
                ignore_project_ids.add(proj_id)
            elif proj_name in project_names:
                # Project ID given directly
                ignore_project_ids.add(proj_name)
            else:
                print(f"Warning: Project '{proj_name}' not found in projects list.")

        instance_usage = calculate_usage_per_instance(
            nova_db_conn,
            nova_api_db_conn,
            start_time,
            end_time,
            project_id,
            ignore_project_ids,
        )

        project_usage = aggregate_usage_by_project(
            instance_usage, project_names, ignore_project_ids
        )

        # Process volume data only if the flag is acknowledged
        if args.acknowledge_no_historical_data:
            volume_type_names = get_volume_type_names(cinder_db_conn)
            volumes = get_extant_volumes(
                cinder_db_conn, start_time, end_time, project_id, ignore_project_ids
            )
            volume_usage = calculate_volume_usage_per_volume(
                volumes, start_time, end_time, volume_type_names
            )
            project_volume_usage = aggregate_volume_usage_by_project(
                volume_usage, ignore_project_ids
            )

            # In debug mode, get reservations data
            if debug:
                reservations = get_reservations(
                    cinder_db_conn, start_time, end_time, project_id, ignore_project_ids
                )
            else:
                reservations = None

            # Merge volume usage into project_usage
            for proj_id, volume_data in project_volume_usage.items():
                if proj_id in ignore_project_ids:
                    continue  # Skip ignored projects
                if proj_id in project_usage:
                    project_usage[proj_id]["volume_gib_hours_per_type"] = volume_data[
                        "gib_hours_per_type"
                    ]
                    project_usage[proj_id]["volumes"] = volume_data["volumes"]
                else:
                    project_usage[proj_id] = {
                        "project_name": project_names.get(proj_id, "Unknown Project"),
                        "cpu_hours": 0,
                        "ram_gib_hours": 0,
                        "volume_gib_hours_per_type": volume_data["gib_hours_per_type"],
                        "machines": [],
                        "volumes": volume_data["volumes"],
                        "images": [],
                        "image_gib_hours": 0,
                        "snapshots": [],
                        "snapshot_gib_hours": 0,
                    }
                # Add reservations data if debug and reservations are available
                if debug and reservations:
                    project_reservations = [
                        res for res in reservations if res["project_id"] == proj_id
                    ]
                    project_usage[proj_id]["reservations"] = project_reservations
        else:
            if debug:
                print(
                    "Skipping volume data because historical data cannot be queried without acknowledgement."
                )

        # Get images within the time range
        images = get_extant_images(
            glance_db_conn, start_time, end_time, project_id, ignore_project_ids
        )

        # Calculate image usage per image
        image_usage = calculate_image_usage_per_image(images, start_time, end_time)

        # Aggregate image usage by project
        project_image_usage = aggregate_image_usage_by_project(
            image_usage, ignore_project_ids
        )

        # Merge image usage into project_usage
        for proj_id, image_data in project_image_usage.items():
            if proj_id in ignore_project_ids:
                continue  # Skip ignored projects
            if proj_id in project_usage:
                project_usage[proj_id]["image_gib_hours"] = image_data["gib_hours"]
                project_usage[proj_id]["images"] = image_data["images"]
            else:
                project_usage[proj_id] = {
                    "project_name": project_names.get(proj_id, "Unknown Project"),
                    "cpu_hours": 0,
                    "ram_gib_hours": 0,
                    "volume_gib_hours_per_type": {},
                    "machines": [],
                    "volumes": [],
                    "image_gib_hours": image_data["gib_hours"],
                    "images": image_data["images"],
                    "snapshots": [],
                    "snapshot_gib_hours": 0,
                }

        # Process snapshots data only if the flag is acknowledged
        if args.acknowledge_no_historical_data:
            snapshots = get_extant_snapshots(
                cinder_db_conn, start_time, end_time, project_id, ignore_project_ids
            )
            snapshot_usage = calculate_snapshot_usage_per_snapshot(
                snapshots, start_time, end_time
            )
            project_snapshot_usage = aggregate_snapshot_usage_by_project(
                snapshot_usage, ignore_project_ids
            )

            # Merge snapshot usage into project_usage
            for proj_id, snapshot_data in project_snapshot_usage.items():
                if proj_id in ignore_project_ids:
                    continue  # Skip ignored projects
                if proj_id in project_usage:
                    project_usage[proj_id]["snapshot_gib_hours"] = snapshot_data[
                        "gib_hours"
                    ]
                    project_usage[proj_id]["snapshots"] = snapshot_data["snapshots"]
                else:
                    project_usage[proj_id] = {
                        "project_name": project_names.get(proj_id, "Unknown Project"),
                        "cpu_hours": 0,
                        "ram_gib_hours": 0,
                        "volume_gib_hours_per_type": {},
                        "machines": [],
                        "volumes": [],
                        "snapshots": snapshot_data["snapshots"],
                        "snapshot_gib_hours": snapshot_data["gib_hours"],
                        "images": [],
                        "image_gib_hours": 0,
                    }
        else:
            if debug:
                print(
                    "Skipping snapshot data because historical data cannot be queried without acknowledgement."
                )

        # Octavia data if flags are provided
        if args.octavia and args.acknowledge_no_historical_data:
            octavia_db_conn = get_database_connection("octavia")
            try:
                # If project_id is specified, use it; otherwise, use all project IDs from project_usage
                if project_id:
                    project_ids = [project_id]
                else:
                    project_ids = list(project_usage.keys())
                # Exclude ignored projects
                project_ids = [
                    pid for pid in project_ids if pid not in ignore_project_ids
                ]
                # Get quotas for projects
                quota_results = get_quotas_for_projects(octavia_db_conn, project_ids)
                quotas_by_project = {
                    quota["project_id"]: quota for quota in quota_results
                }

                for proj_id in project_ids:
                    if proj_id in ignore_project_ids:
                        continue  # Skip ignored projects
                    quota = quotas_by_project.get(proj_id)
                    if quota and is_meaningful_octavia_quota(quota):
                        project_usage.setdefault(
                            proj_id,
                            {
                                "project_name": project_names.get(
                                    proj_id, "Unknown Project"
                                ),
                                "cpu_hours": 0,
                                "ram_gib_hours": 0,
                                "machines": [],
                                "volumes": [],
                                "volume_gib_hours_per_type": {},
                                "images": [],
                                "image_gib_hours": 0,
                                "snapshots": [],
                                "snapshot_gib_hours": 0,
                            },
                        )
                        project_usage[proj_id]["octavia_quota"] = quota
                        # Get load balancer details
                        load_balancers = get_load_balancer_details_per_project(
                            octavia_db_conn, proj_id
                        )
                        if load_balancers:
                            project_usage[proj_id]["load_balancers"] = load_balancers
            finally:
                octavia_db_conn.close()
        elif args.octavia and not args.acknowledge_no_historical_data:
            print(
                "Error: You must acknowledge that historical data cannot be queried for volumes and Octavia by using the --yes-i-know-that-i-can-not-query-historical-data flag."
            )
            return

        # Calculate total duration hours
        total_duration_hours = (end_time - start_time).total_seconds() / 3600

        # Add timeframe to each project
        for proj_id in project_usage:
            project_usage[proj_id]["timeframe"] = {
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "duration_hours": total_duration_hours,
            }

        # Output the usage data in JSON format
        print(json.dumps(project_usage, indent=4, default=str))

    finally:
        nova_db_conn.close()
        nova_api_db_conn.close()
        keystone_db_conn.close()
        cinder_db_conn.close()
        glance_db_conn.close()


if __name__ == "__main__":
    main()
