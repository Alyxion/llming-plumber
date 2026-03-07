"""Tests for the safe expression evaluator and template renderer.

Covers:
- Arithmetic, comparisons, boolean logic
- Variable lookup, subscript, ternary
- Whitelisted function calls
- f-string support
- Template rendering with {expression} placeholders
- ALL injection and resource exhaustion attacks
"""

from __future__ import annotations

import pytest

from llming_plumber.blocks.core.safe_eval import (
    SafeEvalError,
    render_template,
    safe_eval,
)

# ------------------------------------------------------------------
# Basic arithmetic
# ------------------------------------------------------------------


class TestArithmetic:
    def test_addition(self) -> None:
        assert safe_eval("2 + 3") == 5

    def test_subtraction(self) -> None:
        assert safe_eval("10 - 4") == 6

    def test_multiplication(self) -> None:
        assert safe_eval("3 * 7") == 21

    def test_division(self) -> None:
        assert safe_eval("10 / 3") == pytest.approx(3.333, rel=0.01)

    def test_floor_division(self) -> None:
        assert safe_eval("10 // 3") == 3

    def test_modulo(self) -> None:
        assert safe_eval("10 % 3") == 1

    def test_power(self) -> None:
        assert safe_eval("2 ** 10") == 1024

    def test_negative(self) -> None:
        assert safe_eval("-5") == -5

    def test_nested_arithmetic(self) -> None:
        assert safe_eval("(2 + 3) * 4 - 1") == 19

    def test_float_literal(self) -> None:
        assert safe_eval("3.14 * 2") == pytest.approx(6.28)


# ------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------


class TestVariables:
    def test_simple_variable(self) -> None:
        assert safe_eval("x", {"x": 42}) == 42

    def test_variable_in_expression(self) -> None:
        assert safe_eval("x + 1", {"x": 10}) == 11

    def test_multiple_variables(self) -> None:
        assert safe_eval("a + b", {"a": 3, "b": 7}) == 10

    def test_unknown_variable(self) -> None:
        with pytest.raises(SafeEvalError, match="Unknown variable"):
            safe_eval("unknown_var")

    def test_string_variable(self) -> None:
        assert safe_eval("name", {"name": "Alice"}) == "Alice"

    def test_dict_variable(self) -> None:
        data = {"info": {"key": "val"}}
        assert safe_eval("info", data) == {"key": "val"}


# ------------------------------------------------------------------
# Comparisons and boolean logic
# ------------------------------------------------------------------


class TestComparisons:
    def test_equal(self) -> None:
        assert safe_eval("x == 5", {"x": 5}) is True

    def test_not_equal(self) -> None:
        assert safe_eval("x != 5", {"x": 3}) is True

    def test_less_than(self) -> None:
        assert safe_eval("x < 10", {"x": 3}) is True

    def test_greater_equal(self) -> None:
        assert safe_eval("x >= 10", {"x": 10}) is True

    def test_chained_comparison(self) -> None:
        assert safe_eval("1 < x < 10", {"x": 5}) is True
        assert safe_eval("1 < x < 10", {"x": 15}) is False

    def test_and(self) -> None:
        assert safe_eval("x > 0 and x < 10", {"x": 5}) is True

    def test_or(self) -> None:
        assert safe_eval("x < 0 or x > 10", {"x": 15}) is True

    def test_not(self) -> None:
        assert safe_eval("not x", {"x": False}) is True

    def test_ternary(self) -> None:
        assert safe_eval(
            "'yes' if x > 0 else 'no'", {"x": 5},
        ) == "yes"
        assert safe_eval(
            "'yes' if x > 0 else 'no'", {"x": -1},
        ) == "no"


# ------------------------------------------------------------------
# Subscript (dict/list access)
# ------------------------------------------------------------------


class TestSubscript:
    def test_dict_access(self) -> None:
        assert safe_eval(
            "data['name']", {"data": {"name": "test"}},
        ) == "test"

    def test_list_access(self) -> None:
        assert safe_eval("items[0]", {"items": [10, 20, 30]}) == 10

    def test_nested_access(self) -> None:
        data = {"users": [{"name": "Alice"}]}
        assert safe_eval(
            "users[0]['name']", {"users": data["users"]},
        ) == "Alice"

    def test_invalid_key(self) -> None:
        with pytest.raises(SafeEvalError, match="Subscript error"):
            safe_eval("data['missing']", {"data": {}})

    def test_index_out_of_range(self) -> None:
        with pytest.raises(SafeEvalError, match="Subscript error"):
            safe_eval("items[99]", {"items": [1, 2]})


# ------------------------------------------------------------------
# Function calls (whitelisted only)
# ------------------------------------------------------------------


class TestFunctionCalls:
    def test_str(self) -> None:
        assert safe_eval("str(42)") == "42"

    def test_int(self) -> None:
        assert safe_eval("int('7')") == 7

    def test_float(self) -> None:
        assert safe_eval("float('3.14')") == pytest.approx(3.14)

    def test_len(self) -> None:
        assert safe_eval("len(items)", {"items": [1, 2, 3]}) == 3

    def test_abs(self) -> None:
        assert safe_eval("abs(-5)") == 5

    def test_min_max(self) -> None:
        assert safe_eval("min(3, 1, 2)") == 1
        assert safe_eval("max(3, 1, 2)") == 3

    def test_round(self) -> None:
        assert safe_eval("round(3.14159, 2)") == 3.14

    def test_bool(self) -> None:
        assert safe_eval("bool(1)") is True
        assert safe_eval("bool(0)") is False

    def test_disallowed_function(self) -> None:
        with pytest.raises(SafeEvalError, match="Function not allowed"):
            safe_eval("eval('1+1')")

    def test_print_blocked(self) -> None:
        with pytest.raises(SafeEvalError, match="Function not allowed"):
            safe_eval("print('hi')")

    def test_method_call_blocked(self) -> None:
        with pytest.raises(SafeEvalError, match="Only simple function"):
            safe_eval("'hello'.upper()")


# ------------------------------------------------------------------
# Literals
# ------------------------------------------------------------------


class TestLiterals:
    def test_string(self) -> None:
        assert safe_eval("'hello'") == "hello"

    def test_none(self) -> None:
        assert safe_eval("None") is None

    def test_bool(self) -> None:
        assert safe_eval("True") is True

    def test_list_literal(self) -> None:
        assert safe_eval("[1, 2, 3]") == [1, 2, 3]

    def test_dict_literal(self) -> None:
        assert safe_eval("{'a': 1}") == {"a": 1}

    def test_tuple_literal(self) -> None:
        assert safe_eval("(1, 2)") == (1, 2)


# ------------------------------------------------------------------
# String operations
# ------------------------------------------------------------------


class TestStringOps:
    def test_string_concat(self) -> None:
        assert safe_eval(
            "'Hello ' + name", {"name": "World"},
        ) == "Hello World"

    def test_string_repeat(self) -> None:
        assert safe_eval("'abc' * 3") == "abcabcabc"

    def test_string_in_expression(self) -> None:
        assert safe_eval(
            "str(index) + '. ' + name",
            {"index": 1, "name": "Alice"},
        ) == "1. Alice"


# ------------------------------------------------------------------
# INJECTION AND RESOURCE EXHAUSTION — must all be rejected
# ------------------------------------------------------------------


class TestInjectionPrevention:
    def test_import_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("__import__('os')")

    def test_eval_blocked(self) -> None:
        with pytest.raises(SafeEvalError, match="Function not allowed"):
            safe_eval("eval('1+1')")

    def test_exec_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("exec('x=1')")

    def test_attribute_access_blocked(self) -> None:
        with pytest.raises(SafeEvalError, match="Unsupported"):
            safe_eval("''.__class__")

    def test_dunder_access_blocked(self) -> None:
        with pytest.raises(SafeEvalError, match="Unsupported"):
            safe_eval("x.__class__.__bases__", {"x": ""})

    def test_globals_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("globals()")

    def test_locals_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("locals()")

    def test_open_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("open('/etc/passwd')")

    def test_compile_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("compile('1+1', '', 'eval')")

    def test_lambda_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("(lambda: 1)()")

    def test_comprehension_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("[x for x in range(10)]")

    def test_walrus_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("(x := 42)")

    def test_starred_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("(*[1,2,3],)")

    def test_multiline_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("1\n2")


class TestResourceExhaustion:
    def test_huge_string_repeat(self) -> None:
        with pytest.raises(SafeEvalError, match="repeat.*limit"):
            safe_eval("'x' * 10000000")

    def test_huge_string_reverse_repeat(self) -> None:
        with pytest.raises(SafeEvalError, match="repeat.*limit"):
            safe_eval("10000000 * 'x'")

    def test_huge_list_repeat(self) -> None:
        with pytest.raises(SafeEvalError, match="repeat.*limit"):
            safe_eval("[1] * 10000000")

    def test_huge_power(self) -> None:
        with pytest.raises(SafeEvalError, match="Exponent.*limit"):
            safe_eval("2 ** 999999")

    def test_huge_number(self) -> None:
        with pytest.raises(SafeEvalError, match="Numeric result"):
            safe_eval("10 ** 16")

    def test_long_expression(self) -> None:
        with pytest.raises(SafeEvalError, match="too long"):
            safe_eval("x + " * 500 + "1", {"x": 1})

    def test_nested_power(self) -> None:
        """2 ** 2 ** 100 — inner power is huge."""
        with pytest.raises(SafeEvalError):
            safe_eval("2 ** (2 ** 100)")

    def test_infinity_blocked(self) -> None:
        with pytest.raises(SafeEvalError):
            safe_eval("1e308 * 10")

    def test_nan_blocked(self) -> None:
        # 0.0 / 0.0 is a ZeroDivisionError in Python, also caught
        with pytest.raises((SafeEvalError, ZeroDivisionError)):
            safe_eval("0.0 / 0.0")

    def test_huge_string_via_concat(self) -> None:
        """Building a huge string via repeated concat."""
        # 'x' * 999 is OK, then trying to repeat again should fail
        with pytest.raises(SafeEvalError):
            safe_eval("('x' * 999) * 999")


# ------------------------------------------------------------------
# Template rendering
# ------------------------------------------------------------------


class TestRenderTemplate:
    def test_simple_variable(self) -> None:
        assert render_template(
            "Hello {name}!", {"name": "World"},
        ) == "Hello World!"

    def test_expression_in_template(self) -> None:
        assert render_template(
            "Item #{index + 1}", {"index": 0},
        ) == "Item #1"

    def test_multiple_expressions(self) -> None:
        result = render_template(
            "{a} + {b} = {a + b}", {"a": 3, "b": 4},
        )
        assert result == "3 + 4 = 7"

    def test_escaped_braces(self) -> None:
        assert render_template(
            "Use {{braces}} for literal", {},
        ) == "Use {braces} for literal"

    def test_no_expressions(self) -> None:
        assert render_template("plain text", {}) == "plain text"

    def test_ternary_in_template(self) -> None:
        result = render_template(
            "Status: {'even' if x % 2 == 0 else 'odd'}",
            {"x": 4},
        )
        assert result == "Status: even"

    def test_function_in_template(self) -> None:
        result = render_template(
            "Length: {len(items)}", {"items": [1, 2, 3]},
        )
        assert result == "Length: 3"

    def test_template_too_long(self) -> None:
        with pytest.raises(SafeEvalError, match="Template too long"):
            render_template("x" * 100_000, {})

    def test_too_many_expressions(self) -> None:
        template = " ".join(f"{{{i}}}" for i in range(200))
        variables = {str(i): i for i in range(200)}
        with pytest.raises(SafeEvalError, match="Too many"):
            render_template(template, variables)

    def test_injection_in_template(self) -> None:
        """Expressions inside templates are also safe-eval'd."""
        result = render_template(
            "Result: {value}", {"value": "safe"},
        )
        assert result == "Result: safe"

        with pytest.raises(SafeEvalError):
            render_template(
                "{__import__('os').system('rm -rf /')}", {},
            )

    def test_counter_hello_world(self) -> None:
        """The hello world use case: loop counter in a message."""
        messages = []
        for i in range(5):
            msg = render_template(
                "Hello #{index + 1} of {total}!",
                {"index": i, "total": 5},
            )
            messages.append(msg)
        assert messages == [
            "Hello #1 of 5!",
            "Hello #2 of 5!",
            "Hello #3 of 5!",
            "Hello #4 of 5!",
            "Hello #5 of 5!",
        ]
