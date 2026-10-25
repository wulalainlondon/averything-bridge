# Reviewed artifact-verifier test fixture

These three exact compiled modules are the reviewed verifier-fixture157 used by the qualified Go176 full regression. They are test-only code, not provider credentials or live caller authority. SHA256SUMS pins the exact bytes; source tests still verify wrong pins, foreign scope and changed ownership are refused.

For the complete source suite, install Node26.7.0, run npm ci --ignore-scripts in services/line-account and services/line-voice, build services/line-account, verify this SHA256SUMS from the repository root, then set OWNED_VERIFIER_SCRIPT to this directory's absolute artifact-owner-verifier.mjs path before go test ./.... This environment prerequisite is mandatory; do not skip the verifier tests or substitute an echo adapter.

No Go production source or signed package was changed to supply this public CI fixture.
