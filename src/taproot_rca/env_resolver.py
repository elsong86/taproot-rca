"""
Environment variable resolver.

Interpolates ${VAR_NAME} placeholders in strings with values
from the environment. Raises clear errors for missing variables.
"""

from __future__ import annotations

import os
import re


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def resolve_env_vars(value: str) -> str:
    """
    Replace all ${VAR_NAME} placeholders with environment variable values.

    Raises:
        EnvironmentError: If a referenced variable is not set.
    """
    missing: list[str] = []

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        env_val = os.environ.get(var_name)
        if env_val is None:
            missing.append(var_name)
            return match.group(0)  # leave placeholder for error message
        return env_val

    result = _ENV_PATTERN.sub(_replace, value)

    if missing:
        vars_list = ", ".join(f"${{{v}}}" for v in missing)
        raise EnvironmentError(
            f"Missing environment variable(s): {vars_list}\n"
            f"  Set them before running taproot, e.g.:\n"
            f"  export {missing[0]}=your_value"
        )

    return result