from pathlib import Path
import unittest

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCKER_ROOT = PROJECT_ROOT / "orchestration" / "docker"


class ContainerizationTests(unittest.TestCase):
    def test_compose_has_one_batch_service_and_persistent_state(self):
        compose = yaml.safe_load(
            (DOCKER_ROOT / "compose.yaml").read_text(encoding="utf-8")
        )

        self.assertEqual(set(compose["services"]), {"batch"})
        batch = compose["services"]["batch"]
        self.assertEqual(batch["build"]["context"], "../..")
        self.assertEqual(
            batch["build"]["dockerfile"], "orchestration/docker/Dockerfile"
        )
        self.assertIn("../../project.env", batch["env_file"])
        self.assertIn("reconciliation-state:/app/state", batch["volumes"])
        self.assertIn("reconciliation-state", compose["volumes"])
        self.assertIn("/app/state/reconciliation.duckdb", batch["command"])
        self.assertIn("/app/state/data", batch["command"])
        self.assertTrue(batch["read_only"])

    def test_image_is_non_root_and_cannot_receive_local_secrets(self):
        dockerfile = (DOCKER_ROOT / "Dockerfile").read_text(encoding="utf-8")
        dockerignore = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")

        self.assertIn("USER app", dockerfile)
        self.assertNotIn("COPY . ", dockerfile)
        self.assertNotIn("project.env", dockerfile)
        self.assertTrue(dockerignore.lstrip().startswith("# Deny by default"))
        self.assertIn("\n**\n", dockerignore)
        self.assertNotIn("!project.env", dockerignore)


if __name__ == "__main__":
    unittest.main()
