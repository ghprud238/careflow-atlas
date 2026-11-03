"""Explicit error classes for CareFlow Atlas platform and contracts."""

class CareFlowPlatformError(Exception):
    """Base exception for all CareFlow Atlas platform errors."""
    pass


class ContractViolationError(CareFlowPlatformError):
    """Raised when data or payload violates strict schema or invariant rules."""
    pass


class DeliveryConflictError(CareFlowPlatformError):
    """Raised when a batch ID conflicts with a previously received batch of different content."""
    pass


class DuplicateDeliveryError(CareFlowPlatformError):
    """Raised when an exact duplicate batch or manifest is delivered."""
    pass


class ConflictingDeliveryError(DeliveryConflictError):
    """Explicit alias for conflicting delivery (same batch ID, differing checksum)."""
    pass


class ConservationViolationError(CareFlowPlatformError):
    """Raised when the parsed-resource conservation invariant is violated.
    
    Invariant: N_input == N_accepted + N_duplicate + N_quarantined + N_intentionally_excluded
    """
    pass


class AuthorizationDeniedError(CareFlowPlatformError):
    """Raised when an operation is attempted across unauthorized scopes or roles."""
    pass


class CrossProviderAccessError(AuthorizationDeniedError):
    """Raised when a request attempts to access or mutate cross-provider resources."""
    pass


class StaleApprovalError(CareFlowPlatformError):
    """Raised when an approval artifact is expired or references stale registry/evidence versions."""
    pass


class ApprovalHashMismatchError(CareFlowPlatformError):
    """Raised when an approved proposal hash does not match the executed proposal artifact."""
    pass


class UnsupportedDSLOperationError(CareFlowPlatformError):
    """Raised when a mapping DSL specifies an unapproved or unsupported operation."""
    pass


class SnapshotRollbackError(CareFlowPlatformError):
    """Raised when an invalid or non-existent snapshot rollback is attempted."""
    pass
