"""Deterministic Declarative Mapping DSL Interpreter.

Executes strictly allowlisted clinical mapping operations:
- rename_field
- parse_decimal
- map_approved_code
- convert_approved_unit

Strictly prohibits eval, exec, code generation, SQL, or arbitrary regex.
"""

from typing import Any, Dict, Optional, Tuple
from contracts import MappingDSL, UnsupportedDSLOperationError


UNIT_CONVERSION_FACTORS = {
    # Approved clinical conversion factors (Glucose mmol/L <-> mg/dL)
    ("mmol/L", "mg/dL", "glucose"): 18.0182,
    ("mg/dL", "mmol/L", "glucose"): 1.0 / 18.0182,
}


class DSLInterpreter:
    """Safe, sandboxed interpreter for declarative mapping DSL rules."""

    def apply_observation_rule(
        self,
        raw_code: str,
        raw_val_str: str,
        raw_unit: Optional[str],
        dsl: MappingDSL,
    ) -> Tuple[bool, Optional[str], Optional[float], Optional[str], Optional[str]]:
        """Apply DSL rule to raw observation attributes.
        
        Returns:
            (is_matched, normalized_concept, normalized_value, normalized_unit, error_message)
        """
        # 1. Match Evaluation
        target_code = dsl.match.get("local_test_code") or dsl.match.get("code")
        if not target_code or target_code != raw_code:
            return False, None, None, None, None

        # 2. Value Operation (parse_decimal)
        norm_val: Optional[float] = None
        if dsl.value_operation == "parse_decimal":
            try:
                # Handle comma decimal separator and whitespace safely
                sanitized_str = raw_val_str.strip().replace(",", ".")
                norm_val = float(sanitized_str)
            except (ValueError, TypeError):
                return True, None, None, None, f"Failed to parse decimal from '{raw_val_str}'"
        elif dsl.value_operation == "none":
            norm_val = None
        else:
            raise UnsupportedDSLOperationError(f"Unsupported value_operation: {dsl.value_operation}")

        # 3. Unit Validation and Conversion
        norm_unit = raw_unit
        if dsl.source_unit and raw_unit and dsl.target_unit:
            if raw_unit != dsl.source_unit:
                return True, None, None, None, f"Mismatched source unit: expected '{dsl.source_unit}', got '{raw_unit}'"

            if dsl.source_unit != dsl.target_unit:
                conv_key = (dsl.source_unit, dsl.target_unit, "glucose")
                if conv_key in UNIT_CONVERSION_FACTORS and norm_val is not None:
                    norm_val = round(norm_val * UNIT_CONVERSION_FACTORS[conv_key], 2)
                    norm_unit = dsl.target_unit
                else:
                    return True, None, None, None, f"Unsupported unit conversion from {dsl.source_unit} to {dsl.target_unit}"
            else:
                norm_unit = dsl.target_unit

        return True, dsl.target_concept_ref, norm_val, norm_unit, None
