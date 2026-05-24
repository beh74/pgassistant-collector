curl -sS -X POST "http://localhost:8081/collect_all" \
  -H "Content-Type: application/json" \
  -d '{
    "source_path": "config/sources.yaml",
    "include_disabled": false,
    "metadata": {
      "triggered_by": "manual-curl"
    }
  }'