# Navigate to your export directory
cd C:\databricks-export

# Function to clean secrets from files
function Clean-Secrets {
    param($Directory)

    # Get all Python files
    $files = Get-ChildItem -Path $Directory -Filter "*.py" -Recurse

    foreach ($file in $files) {
        Write-Host "Cleaning: $($file.FullName)"

        # Read file content
        $content = Get-Content $file.FullName -Raw

        # Replace OpenAI API keys (sk-...)
        $content = $content -replace 'sk-[a-zA-Z0-9]{20,}', 'YOUR_OPENAI_API_KEY'

        # Replace Databricks tokens (dapi...)
        $content = $content -replace 'dapi[a-zA-Z0-9]{32,}', 'YOUR_DATABRICKS_TOKEN'

        # Replace workspace URLs with tokens
        $content = $content -replace 'https://[^/]+\.cloud\.databricks\.com/.*\?token=[^"&\s]+', 'https://YOUR_WORKSPACE.cloud.databricks.com'

        # Replace hardcoded workspace URLs
        $content = $content -replace 'dbc-[a-zA-Z0-9-]+\.cloud\.databricks\.com', 'YOUR_WORKSPACE.cloud.databricks.com'

        # Replace any remaining token patterns
        $content = $content -replace '"[a-zA-Z0-9]{40,}"', '"YOUR_TOKEN_HERE"'

        # Write cleaned content back
        Set-Content -Path $file.FullName -Value $content -Encoding UTF8
    }
}

# Run the cleaning function
Clean-Secrets -Directory "."

Write-Host "✅ Secret cleaning completed!"