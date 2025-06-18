from pandas import DataFrame

from datasets.vcdb.helpers import undo_onehot_encoding, filter_columns, get_vcdb, first_true_column, coalesce


def get_actor_varieties() -> DataFrame:
    cols = filter_columns(include_patterns=["actor.*.variety.*", "actor.*.motive.*"])
    actors_varieties = get_vcdb(cols)

    actor_types = ["internal", "external", "partner"]

    for actor in actor_types:
        actors_varieties[actor] = undo_onehot_encoding(actors_varieties[cols], f"actor.{actor}")
    cols = [s for s in cols if s not in "incident_id"]

    motive_cols = [col for col in cols if "motive" in col]

    actors_varieties['motive'] = first_true_column(actors_varieties, motive_cols).apply(
        lambda x: None if x is None else x.split(".")[-1])
    actors_varieties['actor'] = first_true_column(actors_varieties, actor_types)
    actors_varieties['actor_variety'] = coalesce(actors_varieties, actor_types)

    actors_varieties.drop(columns=cols + actor_types, inplace=True)

    return actors_varieties


def get_assets_varieties() -> DataFrame:
    cols = filter_columns(include_patterns=["asset.*"], exclude_patterns=["asset.assets.amount.*", "asset.country.*"])
    assets = get_vcdb(cols)

    cols = [s for s in cols if s not in ("incident_id", "asset.total_amount")]

    assets["asset_cloud"] = undo_onehot_encoding(assets[cols], "asset.cloud")
    assets["asset_hosting"] = undo_onehot_encoding(assets[cols], "asset.hosting")
    assets["asset_management"] = undo_onehot_encoding(assets[cols], "asset.management")
    assets["asset_ownership"] = undo_onehot_encoding(assets[cols], "asset.ownership")
    assets["asset_role"] = undo_onehot_encoding(assets[cols], "asset.role")
    assets["asset_variety"] = undo_onehot_encoding(assets[cols], "asset.assets.variety").apply(
        lambda x: x.split(" - ")[-1])

    assets.drop(columns=cols, inplace=True)

    return assets


def get_action_varieties() -> DataFrame:
    cols = filter_columns(include_patterns=["action.*.variety.*"])
    action_varieties = get_vcdb(cols)

    action_types = ["environmental", "error", "hacking", "malware", "misuse", "physical", "social"]

    for action in action_types:
        action_varieties[action] = undo_onehot_encoding(action_varieties[cols], f"action.{action}")
    cols = [s for s in cols if s not in "incident_id"]

    action_varieties.drop(columns=cols, inplace=True)

    action_varieties['action'] = first_true_column(action_varieties, action_types)
    action_varieties['action_variety'] = coalesce(action_varieties, action_types)
    action_varieties.drop(columns=action_types, inplace=True)

    return action_varieties


def get_location() -> DataFrame:
    cols = filter_columns(include_patterns=["*.country.*", "actor.external.country.*"])
    location = get_vcdb(cols)

    cols = [s for s in cols if s != "incident_id"]

    location["external_actor_country"] = undo_onehot_encoding(location[cols], "actor.external.country")
    location["asset_country"] = undo_onehot_encoding(location[cols], "asset")
    location["victim_country"] = undo_onehot_encoding(location[cols], "victim")

    location.drop(columns=cols, inplace=True)

    return location


def get_timeline() -> DataFrame:
    timeline = get_vcdb(["incident_id", "timeline.incident.year", "timeline.incident.month", "timeline.incident.day"])
    timeline.rename({x: x.split(".")[-1] for x in timeline.columns}, inplace=True, axis=1)
    timeline["day"] = timeline["day"].astype("Int8")
    timeline["month"] = timeline["month"].astype("Int8")

    return timeline


def get_action_notes() -> DataFrame:
    cols = filter_columns(include_patterns=["action.*.notes"])
    action_notes = get_vcdb(cols)

    cols.remove("incident_id")
    action_notes["action_notes"] = coalesce(action_notes, cols)

    action_notes.drop(cols, axis=1, inplace=True)

    return action_notes
