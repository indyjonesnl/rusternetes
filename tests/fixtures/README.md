# Test Fixtures

This directory contains YAML manifests used for testing k3rs functionality.

## Available Fixtures

- **test-nginx-pod.yaml** - Sample nginx pod for testing
- **test-sa.yaml** - Service account test fixture
- **test-ns.yaml** - Namespace test fixture
- **test-sa-injection.yaml** - Service account injection test
- **test-default-sa.yaml** - Default service account test

## Usage

These fixtures can be applied using kubectl:

```bash
# Example: Apply a test pod
./target/release/kubectl apply -f tests/fixtures/test-nginx-pod.yaml

# Example: Apply a test namespace
./target/release/kubectl apply -f tests/fixtures/test-ns.yaml
```

## Purpose

These fixtures are used by test scripts and for manual testing of k3rs features. They are kept separate from the examples directory as they are specifically for testing rather than demonstration purposes.
