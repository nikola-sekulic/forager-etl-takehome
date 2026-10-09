import unittest

from mapping import INDEX_DEFINITION, index_definition


class ShardConfigurationTests(unittest.TestCase):
    def test_shard_tuning_preserves_mapping_durability_and_other_runs(self):
        tuned = index_definition(8)
        self.assertEqual(tuned["mappings"], INDEX_DEFINITION["mappings"])
        self.assertEqual(tuned["settings"]["translog.durability"], "request")
        self.assertEqual(tuned["settings"]["number_of_replicas"], 0)
        tuned["settings"]["number_of_shards"] = 2
        self.assertEqual(index_definition()["settings"]["number_of_shards"], 4)
        self.assertEqual(INDEX_DEFINITION["settings"]["number_of_shards"], 4)

    def test_shard_count_is_bounded_and_excludes_boolean_ids(self):
        for invalid in (0, 9, True, 1.5):
            with self.subTest(value=invalid), self.assertRaises(ValueError):
                index_definition(invalid)


if __name__ == "__main__":
    unittest.main()
