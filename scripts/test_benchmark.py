import unittest
from benchmark import cpu_seconds, recommendation


class MeasurementTests(unittest.TestCase):
    def test_cpu_time_formats(self):
        self.assertAlmostEqual(cpu_seconds("02:03.45"), 123.45)
        self.assertEqual(cpu_seconds("1-02:03:04"), 93784)
        self.assertEqual(cpu_seconds("00:00.01"), .01)

    def test_budget_and_throughput_knee(self):
        def row(workers, throughput, memory, cpu):
            return {"group": "workers", "workers": workers,
                    "tasksPerSecond": throughput, "peakTreeRSSMiB": memory,
                    "meanCPUCores": cpu, "runtime": {"executeP95Ms": 100}}
        results = [row(1, 50, 150, .5), row(2, 96, 300, 1), row(2, 98, 320, 1),
                   row(4, 100, 600, 2), row(8, 120, 1200, 4)]
        # Four is fastest within budget, but two achieves >=95% with less memory.
        self.assertEqual(recommendation(results, 1000, 3)["recommendedWorkers"], 2)
        self.assertEqual(recommendation(results, 200, 3)["recommendedWorkers"], 1)
        self.assertIsNone(recommendation(results, 100, 3)["recommendedWorkers"])
        self.assertEqual(recommendation(results, 2000, 5)["recommendedWorkers"], 8)


if __name__ == "__main__":
    unittest.main()
