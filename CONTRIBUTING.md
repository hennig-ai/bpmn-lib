# Contributing to bpmn-lib

Thank you for your interest in contributing to bpmn-lib!

## Getting Started

1. Fork the repository
2. Clone your fork
3. Install the development dependencies:

```bash
pip install -e ".[test]"
```

4. Create a feature branch from `main`:

```bash
git checkout -b feature/your-feature-name
```

## Development Guidelines

- All code must include type hints for parameters, return values, and attributes
- Follow the existing code style and project conventions
- Do not introduce silent error handling or graceful degradation - fail fast with clear error messages
- Use `log_and_raise()` from `basic_framework` instead of bare `raise` statements

## Running Tests

```bash
pytest tests/ -v
pytest tests/unit/ -v
pytest tests/integration/ -v
```

## Submitting Changes

1. Ensure all tests pass
2. Write clear, concise commit messages
3. Open a pull request against `main`
4. Describe what your change does and why

## Reporting Issues

- Use GitHub Issues to report bugs
- Include steps to reproduce, expected behavior, and actual behavior
- Include the Python version and bpmn-lib version

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
