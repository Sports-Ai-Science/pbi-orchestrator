# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in PBI Orchestrator, please follow these steps:

1. **Do NOT** open a public issue
2. Email the maintainers with details:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

3. Allow time for the maintainers to respond and address the issue
4. Once fixed, a security advisory will be published

## Security Measures

This project implements several security measures:

- **Branch Protection**: Direct pushes to main are disabled
- **Dependency Scanning**: Automated npm audit in CI
- **Code Review**: All changes require PR review
- **Type Safety**: TypeScript strict mode enabled
- **Input Validation**: All user inputs are validated

## Best Practices

When contributing:
- Never commit secrets, API keys, or credentials
- Use environment variables for sensitive configuration
- Validate and sanitize all external inputs
- Follow the principle of least privilege
- Keep dependencies up to date

## Dependency Management

- Dependencies are checked with `npm audit`
- Security updates are applied promptly
- Minimal dependency footprint

Thank you for helping keep PBI Orchestrator secure!
