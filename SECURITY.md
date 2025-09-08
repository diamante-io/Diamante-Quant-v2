# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Currently supported versions:

| Version | Supported          |
| ------- | ------------------ |
| v2.x.x  | :white_check_mark: |
| < 2.0   | :x:                |

## Reporting a Vulnerability

The Diamante team takes security seriously. We appreciate your efforts to responsibly disclose your findings, and will make every effort to acknowledge your contributions.

### How to Report

To report a security vulnerability, please follow these steps:

1. **DO NOT** create a public GitHub issue for security vulnerabilities
2. Email your findings to `info@diamante.io` (encrypted with our PGP key if possible)
3. Include the following information:
   - Type of vulnerability
   - Full paths of source file(s) related to the vulnerability
   - Location of the affected source code (tag/branch/commit or direct URL)
   - Step-by-step instructions to reproduce the issue
   - Proof-of-concept or exploit code (if possible)
   - Impact assessment and potential attack scenarios

### Response Timeline

- **Initial Response**: Within 48 hours
- **Vulnerability Assessment**: Within 7 days
- **Patch Development**: Based on severity (Critical: 24-48h, High: 3-5 days, Medium: 7-14 days)
- **Public Disclosure**: Coordinated with reporter, typically 30-90 days after patch

### Security Vulnerability Classification

We use the following severity levels:

- **Critical**: Network-wide impact, consensus failure, fund loss >$1M
- **High**: Validator compromise, fund loss <$1M, DoS attacks
- **Medium**: Performance degradation, minor fund loss risks
- **Low**: Information disclosure, configuration issues

## Security Features

### Post-Quantum Cryptography

- **ML-KEM (CRYSTALS-Kyber)**: Quantum-resistant key encapsulation
- **ML-DSA (CRYSTALS-Dilithium)**: Quantum-resistant digital signatures
- **SLH-DSA (SPHINCS+)**: Stateless hash-based signatures
- **AES-256-GCM**: Symmetric encryption
- **SHA-3/SHAKE**: Cryptographic hashing

### Network Security

- **TLS 1.3**: All network communications encrypted
- **Rate Limiting**: DDoS protection at multiple layers
- **Byzantine Fault Tolerance**: Resilient to 33% malicious nodes
- **Input Validation**: Comprehensive sanitization and validation

### Key Management

- **Hardware Security Module (HSM)** support for validators
- **Encrypted key storage** with PBKDF2 key derivation
- **Multi-signature** support for high-value accounts
- **Key rotation** mechanisms for long-term security

## Security Best Practices

### For Node Operators

1. **Environment Variables**: Never hardcode secrets; use environment variables
2. **File Permissions**: Restrict access to key files (chmod 600)
3. **Network Security**: Use firewalls and VPNs for validator nodes
4. **Monitoring**: Enable security alerts and audit logging
5. **Updates**: Apply security patches promptly

### For Developers

1. **Code Review**: All code must be peer-reviewed
2. **Testing**: Security-focused test cases required
3. **Dependencies**: Regular vulnerability scanning with tools like `gosec`
4. **Secrets Management**: Use dedicated secret management solutions
5. **Least Privilege**: Minimize permissions and access scope

## Security Audits

We conduct regular security audits:

- **Internal Reviews**: Monthly security assessments
- **External Audits**: Quarterly third-party audits
- **Penetration Testing**: Bi-annual network penetration tests
- **Smart Contract Audits**: Before any major contract deployment

## Bug Bounty Program

We maintain an active bug bounty program for responsible disclosure:

- **Critical**: $10,000 - $100,000
- **High**: $5,000 - $10,000
- **Medium**: $1,000 - $5,000
- **Low**: $100 - $1,000

Details at: [https://diamante.io/bug-bounty](https://diamante.io/bug-bounty)

## Security Tools

Recommended security tools for the codebase:

```bash
# Static analysis
gosec ./...

# Dependency scanning
go list -json -m all | nancy sleuth

# License compliance
go-licenses check ./...

# Code quality
golangci-lint run
```

## Contact

- **Security Email**: info@diamante.io
- **Security Advisories**: [GitHub Security Advisories](https://github.com/diamante-io/diamante-quant-v2/security/advisories)

## Acknowledgments

We thank the following researchers for responsibly disclosing security issues:

- *[Your name could be here]*

---

**Remember**: Security is everyone's responsibility. If you see something, say something.
