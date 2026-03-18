# Contributing to FAANG Real-Time Data Platform

Thank you for your interest in contributing! This project was built by
[Shradha Dubey](https://github.com/shradhadubey) as a portfolio demonstration
of production-grade data engineering on AWS. Contributions that improve the
architecture, fix bugs, or extend functionality are very welcome.

---

##  Ways to Contribute

- **Bug fixes** — something broken in a script or Terraform config
- **New features** — additional Gold metrics, new data sources, dashboard improvements
- **Documentation** — clearer explanations, better examples, typo fixes
- **Performance** — optimisations to the pipeline or query improvements
- **Tests** — additional unit tests or integration test coverage

---

## Getting Started

### 1. Fork and clone the repo

```bash
git clone https://github.com/shradhadubey/faang-real-time-data-platform.git
cd faang-real-time-data-platform
```

### 2. Set up Python 3.11 environment

```powershell
# Windows
py -3.11 -m venv venv311
.\venv311\Scripts\Activate.ps1
pip install -r requirements.txt
```

```bash
# Mac / Linux
python3.11 -m venv venv311
source venv311/bin/activate
pip install -r requirements.txt
```

### 3. Create a feature branch

```bash
git checkout -b feat/your-feature-name
# or
git checkout -b fix/what-you-are-fixing
```

---

## Branch Naming

| Type | Pattern | Example |
|------|---------|---------|
| Feature | `feat/description` | `feat/add-silver-dedup` |
| Bug fix | `fix/description` | `fix/bronze-partition-key` |
| Documentation | `docs/description` | `docs/update-athena-queries` |
| Infrastructure | `infra/description` | `infra/add-sns-alerts` |

---

## Before Submitting a PR

Please make sure your changes pass all of the following:

```bash
# 1. Lint
pip install ruff
ruff check .

# 2. Format check
ruff format . --check

# 3. Unit tests
pytest tests/ -v

# 4. Terraform validation (if you changed .tf files)
cd infrastructure/terraform
terraform fmt -check
terraform validate
```

---

## Pull Request Guidelines

- **One thing per PR** — keep scope focused so reviews are fast
- **Descriptive title** — e.g. `feat: add revenue by device_type to Gold layer`
- **Fill in the PR template** — explain what changed and why
- **Link related issues** — use `Closes #123` in the PR description
- **Screenshots welcome** — if you changed output or queries, show the results

---

## Reporting Bugs

Open a [GitHub Issue](https://github.com/shradhadubey/faang-real-time-data-platform/issues) with:

- What you were trying to do
- The exact error message or unexpected output
- Your OS, Python version, and AWS region
- Steps to reproduce

---

## Suggesting Features

Open a [GitHub Issue](https://github.com/shradhadubey/faang-real-time-data-platform/issues) with the label `enhancement` and describe:

- The problem it solves
- How you'd expect it to work
- Any alternatives you considered

---

## Project Structure Conventions

| Layer | Location | Format |
|-------|----------|--------|
| Event producer | `data-generator/` | Python scripts |
| Bronze/Silver | `streaming/` | Python + Pandas |
| Gold | `batch/` | Python + Pandas |
| Infrastructure | `infrastructure/terraform/` | Terraform HCL |
| SQL | `sql/` | `.sql` files |
| Tests | `tests/` | pytest |

---

## License

By contributing, you agree that your contributions will be licensed under
the [MIT License](LICENSE) that covers this project.

---

Built with ❤️ by [Shradha Dubey](https://github.com/shradhadubey) —
Data Engineer specialising in Serverless AWS Architectures.
