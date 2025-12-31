# Contributing to PBI Orchestrator

Thank you for your interest in contributing to PBI Orchestrator! This document provides guidelines and instructions for contributing.

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help maintain a welcoming environment

## Development Process

### 1. Setup Your Development Environment

```bash
# Fork the repository on GitHub

# Clone your fork
git clone https://github.com/YOUR_USERNAME/pbi-orchestrator.git
cd pbi-orchestrator

# Install dependencies
npm install

# Start Temporal server
./scripts/start-temporal.sh
```

### 2. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Branch naming conventions:
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation updates
- `refactor/` - Code refactoring
- `test/` - Test additions/improvements

### 3. Make Your Changes

- Write clean, readable code
- Follow existing code style
- Add tests for new features
- Update documentation as needed

### 4. Test Your Changes

```bash
# Run linting
npm run lint

# Run formatting check
npm run format:check

# Run unit tests
npm run test:unit

# Run all tests
npm test
```

Ensure:
- All tests pass
- Code coverage remains above 80%
- No linting errors
- Code is properly formatted

### 5. Commit Your Changes

Use clear, descriptive commit messages:

```bash
git commit -m "feat: add new PBI workflow parameter validation"
git commit -m "fix: correct concurrency manager lock release"
git commit -m "docs: update architecture documentation"
```

Commit message format:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting, etc.)
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `chore:` - Build process or auxiliary tool changes

### 6. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub with:
- Clear title describing the change
- Detailed description of what and why
- Reference any related issues
- Screenshots for UI changes (if applicable)

## Pull Request Guidelines

### PR Description Template

```markdown
## Description
Brief description of the changes

## Motivation
Why is this change needed?

## Changes Made
- List of specific changes
- Any breaking changes

## Testing
- How was this tested?
- What test cases were added?

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Code follows project style
- [ ] All CI checks pass
```

### Review Process

1. **Automated Checks**: CI will automatically run
   - Linting
   - Build
   - Unit tests
   - System/E2E tests

2. **Manual Review**: Maintainers will review your code

3. **Auto-merge**: If all checks pass and PR is approved, it will auto-merge to main

## Coding Standards

### TypeScript

- Use TypeScript strict mode
- Provide proper type annotations
- Avoid `any` types
- Use interfaces for data structures

### Code Style

- Follow ESLint configuration
- Use Prettier for formatting
- Maximum line length: 100 characters
- Use single quotes for strings
- Use 2 spaces for indentation

### Documentation

- Add JSDoc comments for public APIs
- Update README.md for user-facing changes
- Update docs/ for architectural changes
- Include inline comments for complex logic

### Testing

- Write unit tests for new functions
- Add integration tests for workflows
- Maintain >80% code coverage
- Test edge cases and error handling

Example test structure:

```typescript
describe('ComponentName', () => {
  describe('methodName', () => {
    it('should do something specific', () => {
      // Arrange
      const input = { ... };
      
      // Act
      const result = methodName(input);
      
      // Assert
      expect(result).toBe(expected);
    });
  });
});
```

## Project-Specific Guidelines

### Concurrency Control

When modifying concurrency logic:
- Ensure 2-stack limit is maintained
- Test with multiple concurrent workflows
- Handle edge cases (errors, timeouts)
- Maintain thread safety

### Temporal Workflows

When adding/modifying workflows:
- Keep workflows deterministic
- Use activities for side effects
- Implement proper retry logic
- Test workflow execution thoroughly

### Security

- Never commit secrets or credentials
- Validate all user inputs
- Use secure dependencies (check `npm audit`)
- Follow security best practices

## Getting Help

- Open an issue for questions
- Check existing documentation
- Review similar PRs for examples
- Ask maintainers for guidance

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
