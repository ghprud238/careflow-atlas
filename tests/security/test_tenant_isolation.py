"""Security tests for PostgreSQL tenant isolation and scoped tool authorization."""

import psycopg2
import pytest
from contracts import CrossProviderAccessError
from services.mcp_tools.tool_server import ScopedMCPTools


class TestTenantIsolation:
    def test_database_schema_permission_denial(self):
        """Provider A role must be denied access to Provider B schema at the SQL engine level."""
        conn_a_str = "postgresql://provider_a_role:provider_a_secret@localhost:5432/careflow_metadata"
        with psycopg2.connect(conn_a_str) as conn:
            with conn.cursor() as cur:
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute("CREATE TABLE provider_b_isolated.leak_attempt (id int);")

        conn_b_str = "postgresql://provider_b_role:provider_b_secret@localhost:5432/careflow_metadata"
        with psycopg2.connect(conn_b_str) as conn:
            with conn.cursor() as cur:
                with pytest.raises(psycopg2.errors.InsufficientPrivilege):
                    cur.execute("CREATE TABLE provider_a_isolated.leak_attempt (id int);")

    def test_scoped_mcp_tools_scope_guard(self):
        """Cross-provider tool calls must fail closed with CrossProviderAccessError."""
        tools = ScopedMCPTools()

        # Provider A trying to access Provider B
        with pytest.raises(CrossProviderAccessError):
            tools.get_safe_profile(
                caller_scope="provider-a",
                provider_id="provider-b",
                target_code="LOCAL_LAB_017",
            )

        with pytest.raises(CrossProviderAccessError):
            tools.get_schema_diff(
                caller_scope="provider-a",
                provider_id="provider-b",
                old_version="v1.0.0",
                new_version="v1.1.0",
            )

        with pytest.raises(CrossProviderAccessError):
            tools.propose_replay(
                caller_scope="provider-a",
                provider_id="provider-b",
                target_code="LOCAL_LAB_017",
            )
