# certs/ — Custom CA certificate directory

Place your internal root CA certificates here **before building the Docker image**.

## Supported formats

- `*.crt` — PEM-encoded certificate
- `*.pem` — PEM-encoded certificate (rename to `.crt` or it won't be picked up by `update-ca-certificates`)

## How it works

During `docker build`, the Dockerfile runs:

```dockerfile
COPY certs/ /usr/local/share/ca-certificates/ontap/
RUN update-ca-certificates
```

This imports your CA into the container's OS trust store, so Python's `ssl` module and `urllib3` will trust it automatically.

## If you also need boto3 to use the CA explicitly

Set in your `.env`:

```env
S3_CA_BUNDLE=/usr/local/share/ca-certificates/ontap/your-ca.crt
```

## Example: export your internal CA from a Windows server

```powershell
# Export from Windows Certificate Store
$cert = Get-ChildItem -Path Cert:\LocalMachine\Root | Where-Object { $_.Subject -like "*YourCA*" }
Export-Certificate -Cert $cert -FilePath ".\certs\internal-root-ca.crt" -Type CERT
```

Then convert if needed:

```bash
openssl x509 -inform DER -in certs/internal-root-ca.crt -out certs/internal-root-ca-pem.crt
```
