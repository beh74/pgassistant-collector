import unittest

from app.models import DbConfig
from app.pgassistant_client import database_identity, parse_conn_str


class DatabaseIdentityTests(unittest.TestCase):
    def test_extracts_identity_from_uri_without_exposing_password(self):
        config = parse_conn_str(
            "postgresql://report%40user:secret@db.example.com:5544/sales%20data?sslmode=require"
        )

        identity = database_identity(config)

        self.assertEqual(identity.host, "db.example.com")
        self.assertEqual(identity.port, 5544)
        self.assertEqual(identity.name, "sales data")
        self.assertEqual(identity.user, "report@user")
        self.assertNotIn("secret", repr(identity))

    def test_uses_default_postgresql_port(self):
        config = parse_conn_str("postgresql://postgres:secret@database.local/application")

        self.assertEqual(database_identity(config).port, 5432)

    def test_extracts_identity_from_legacy_fields(self):
        identity = database_identity(
            DbConfig(
                db_host="database.local",
                db_port=6432,
                db_name="application",
                db_user="collector",
                db_password="secret",
            )
        )

        self.assertEqual(identity.host, "database.local")
        self.assertEqual(identity.port, 6432)
        self.assertEqual(identity.name, "application")
        self.assertEqual(identity.user, "collector")
        self.assertNotIn("secret", repr(identity))


if __name__ == "__main__":
    unittest.main()
