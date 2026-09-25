#!/bin/bash
# Roda automaticamente quando o LocalStack fica pronto.
set -e

for bucket in vendas-raw vendas-curated athena-results; do
  awslocal s3 mb "s3://${bucket}" || true
done

echo "buckets do LocalStack prontos:"
awslocal s3 ls
