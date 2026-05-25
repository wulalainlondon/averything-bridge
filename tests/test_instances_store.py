from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import instances_store


def _write_json(path: str, instances: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"instances": instances}, fh)


def _make_instance(
    name: str = "test",
    port: int = 9000,
    root_dir: str = "",
    data_dir: str = "/tmp/data",
) -> dict:
    return {
        "name": name,
        "port": port,
        "data_dir": data_dir,
        "root_dir": root_dir,
        "backend": "claude",
        "model": "",
        "ollama_host": "",
    }


class TestLoadInstances(unittest.TestCase):
    def test_returns_empty_list_when_file_missing(self):
        result = instances_store.load_instances("/nonexistent/path/instances.json")
        self.assertEqual(result, [])

    def test_returns_instances_from_valid_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp_path = tmp.name

        try:
            _write_json(tmp_path, [_make_instance("alpha", 9001)])
            result = instances_store.load_instances(tmp_path)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["name"], "alpha")
        finally:
            os.unlink(tmp_path)


class TestSaveInstances(unittest.TestCase):
    def test_round_trip(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp_path = tmp.name

        try:
            items = [_make_instance("beta", 9002)]
            instances_store.save_instances(items, tmp_path)
            loaded = instances_store.load_instances(tmp_path)
            self.assertEqual(loaded, items)
        finally:
            os.unlink(tmp_path)

    def test_creates_bak_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp_path = tmp.name

        bak_path = tmp_path + ".bak"
        try:
            _write_json(tmp_path, [_make_instance("original", 9003)])
            instances_store.save_instances([_make_instance("updated", 9003)], tmp_path)
            self.assertTrue(os.path.exists(bak_path))
            bak_data = json.loads(open(bak_path, encoding="utf-8").read())
            self.assertEqual(bak_data["instances"][0]["name"], "original")
        finally:
            for p in (tmp_path, bak_path):
                if os.path.exists(p):
                    os.unlink(p)

    def test_uses_indent2_and_unicode(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp_path = tmp.name

        try:
            item = _make_instance("unicode-名前", 9010)
            instances_store.save_instances([item], tmp_path)
            raw = open(tmp_path, encoding="utf-8").read()
            self.assertIn("  ", raw)           # indent=2 produces 2-space lines
            self.assertIn("unicode-名前", raw)  # ensure_ascii=False
        finally:
            os.unlink(tmp_path)


class TestValidateInstance(unittest.TestCase):
    def test_name_empty(self):
        item = _make_instance("", 9100)
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "name_empty")

    def test_name_blank_whitespace(self):
        item = _make_instance("   ", 9100)
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "name_empty")

    def test_name_duplicate(self):
        existing = [_make_instance("taken", 9200)]
        item = _make_instance("taken", 9201)
        err = instances_store.validate_instance(item, existing)
        self.assertEqual(err, "name_duplicate")

    def test_name_duplicate_allowed_when_flag_set(self):
        existing = [_make_instance("taken", 9202)]
        item = _make_instance("taken", 9202)
        err = instances_store.validate_instance(item, existing, allow_same_name=True)
        self.assertIsNone(err)

    def test_port_invalid_string(self):
        item = _make_instance("porttest", "not-a-port")
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "port_invalid")

    def test_port_invalid_below_range(self):
        item = _make_instance("porttest", 80)
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "port_invalid")

    def test_port_invalid_above_range(self):
        item = _make_instance("porttest", 70000)
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "port_invalid")

    def test_port_in_use(self):
        existing = [_make_instance("other", 9300)]
        item = _make_instance("newone", 9300)
        err = instances_store.validate_instance(item, existing)
        self.assertEqual(err, "port_in_use")

    def test_root_dir_missing(self):
        item = _make_instance("roottest", 9400, root_dir="/nonexistent/path/xyz")
        err = instances_store.validate_instance(item, [])
        self.assertEqual(err, "root_dir_missing")

    def test_root_dir_empty_string_is_valid(self):
        item = _make_instance("roottest", 9401, root_dir="")
        err = instances_store.validate_instance(item, [])
        self.assertIsNone(err)

    def test_root_dir_existing_path_is_valid(self):
        item = _make_instance("roottest", 9402, root_dir="/tmp")
        err = instances_store.validate_instance(item, [])
        self.assertIsNone(err)

    def test_default_immutable_port(self):
        # Existing default instance at port 8766; trying to change its port.
        existing_default = {
            "name": "default",
            "port": 8766,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        item = {
            "name": "default",
            "port": 9999,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        err = instances_store.validate_instance(item, [existing_default], allow_same_name=True)
        self.assertEqual(err, "default_immutable")

    def test_default_port_unchanged_is_valid(self):
        existing_default = {
            "name": "default",
            "port": 8766,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        item = {
            "name": "default",
            "port": 8766,
            "data_dir": "/tmp/new-data",
            "root_dir": "",
        }
        err = instances_store.validate_instance(item, [existing_default], allow_same_name=True)
        self.assertIsNone(err)

    def test_valid_instance_returns_none(self):
        item = _make_instance("ok", 9500)
        err = instances_store.validate_instance(item, [])
        self.assertIsNone(err)


class TestUpsertInstance(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        # Start with an empty instances.json
        _write_json(self.path, [])

    def tearDown(self):
        for p in (self.path, self.path + ".bak"):
            if os.path.exists(p):
                os.unlink(p)

    def test_insert_new_instance(self):
        item = _make_instance("new", 9600)
        ok, err = instances_store.upsert_instance(item, self.path)
        self.assertTrue(ok)
        self.assertIsNone(err)
        loaded = instances_store.load_instances(self.path)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["name"], "new")

    def test_update_existing_instance(self):
        item = _make_instance("existing", 9601)
        instances_store.upsert_instance(item, self.path)

        updated = _make_instance("existing", 9601, data_dir="/tmp/updated")
        ok, err = instances_store.upsert_instance(updated, self.path)
        self.assertTrue(ok)
        self.assertIsNone(err)

        loaded = instances_store.load_instances(self.path)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["data_dir"], "/tmp/updated")

    def test_insert_then_load_round_trip(self):
        item = _make_instance("roundtrip", 9602)
        instances_store.upsert_instance(item, self.path)
        loaded = instances_store.load_instances(self.path)
        self.assertEqual(loaded[0], item)

    def test_duplicate_port_returns_error(self):
        instances_store.upsert_instance(_make_instance("first", 9700), self.path)
        ok, err = instances_store.upsert_instance(_make_instance("second", 9700), self.path)
        self.assertFalse(ok)
        self.assertEqual(err, "port_in_use")

    def test_same_name_second_upsert_is_update_not_error(self):
        # upsert is keyed by name; a second call with the same name is an update.
        instances_store.upsert_instance(_make_instance("dup", 9800), self.path)
        ok, err = instances_store.upsert_instance(_make_instance("dup", 9800, data_dir="/tmp/v2"), self.path)
        self.assertTrue(ok)
        self.assertIsNone(err)
        loaded = instances_store.load_instances(self.path)
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["data_dir"], "/tmp/v2")

    def test_default_instance_port_immutable(self):
        default_item = {
            "name": "default",
            "port": 8766,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        instances_store.upsert_instance(default_item, self.path)
        mutated = {
            "name": "default",
            "port": 9999,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        ok, err = instances_store.upsert_instance(mutated, self.path)
        self.assertFalse(ok)
        self.assertEqual(err, "default_immutable")


class TestDeleteInstance(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        _write_json(self.path, [])

    def tearDown(self):
        for p in (self.path, self.path + ".bak"):
            if os.path.exists(p):
                os.unlink(p)

    def test_delete_existing_instance(self):
        instances_store.upsert_instance(_make_instance("deleteme", 9900), self.path)
        ok, err = instances_store.delete_instance("deleteme", self.path)
        self.assertTrue(ok)
        self.assertIsNone(err)
        loaded = instances_store.load_instances(self.path)
        self.assertEqual(loaded, [])

    def test_delete_not_found(self):
        ok, err = instances_store.delete_instance("ghost", self.path)
        self.assertFalse(ok)
        self.assertEqual(err, "not_found")

    def test_delete_default_by_name_is_immutable(self):
        default_item = {
            "name": "default",
            "port": 8766,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        _write_json(self.path, [default_item])
        ok, err = instances_store.delete_instance("default", self.path)
        self.assertFalse(ok)
        self.assertEqual(err, "default_immutable")

    def test_delete_default_sentinel_by_port_is_immutable(self):
        # Instance is not named "default" but matches the sentinel (port 8766 + empty root_dir).
        sentinel = {
            "name": "main",
            "port": 8766,
            "data_dir": "/tmp",
            "root_dir": "",
        }
        _write_json(self.path, [sentinel])
        ok, err = instances_store.delete_instance("main", self.path)
        self.assertFalse(ok)
        self.assertEqual(err, "default_immutable")


if __name__ == "__main__":
    unittest.main()
