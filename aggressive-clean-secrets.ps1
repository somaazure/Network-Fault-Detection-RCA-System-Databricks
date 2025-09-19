# Enhanced secret cleaning script
cd C:\databricks-export

function Clean-Secrets-Aggressive {
    param($Directory)

    $files = Get-ChildItem -Path $Directory -Filter "*.py" -Recurse

    foreach ($file in $files) {
        Write-Host "Cleaning: $($file.FullName)"

        $content = Get-Content $file.FullName -Raw
        $originalContent = $content

        # More aggressive OpenAI API key patterns
        $content = $content -replace 'sk-proj-[a-zA-Z0-9\-_]{20,}', 'YOUR_OPENAI_API_KEY'
        $content = $content -replace 'sk-[a-zA-Z0-9\-_]{20,}', 'YOUR_OPENAI_API_KEY'

        # More aggressive Databricks token patterns
        $content = $content -replace 'dapi[a-zA-Z0-9]{32,}', 'YOUR_DATABRICKS_TOKEN'
        $content = $content -replace 'dapi[a-zA-Z0-9\-_]{20,}', 'YOUR_DATABRICKS_TOKEN'

        # Any long alphanumeric strings that look like tokens
        $content = $content -replace '"[a-zA-Z0-9\-_]{50,}"', '"YOUR_TOKEN_HERE"'
        $content = $content -replace "'[a-zA-Z0-9\-_]{50,}'", "'YOUR_TOKEN_HERE'"

        # Workspace URLs with tokens
        $content = $content -replace 'https://[^/]+\.cloud\.databricks\.com/.*\?token=[^"&\s]+', 'https://YOUR_WORKSPACE.cloud.databricks.com'
        $content = $content -replace 'https://adb-[0-9]+\.[0-9]+\.azuredatabricks\.net', 'https://YOUR_WORKSPACE.azuredatabricks.net'
        $content = $content -replace 'dbc-[a-zA-Z0-9-]+\.cloud\.databricks\.com', 'YOUR_WORKSPACE.cloud.databricks.com'

        # Any remaining suspicious patterns
        $content = $content -replace 'os\.environ\["OPENAI_API_KEY"\]\s*=\s*"[^"]{30,}"', 'os.environ["OPENAI_API_KEY"] = "YOUR_OPENAI_API_KEY"'
        $content = $content -replace 'os\.environ\["DATABRICKS_TOKEN"\]\s*=\s*"[^"]{30,}"', 'os.environ["DATABRICKS_TOKEN"] = "YOUR_DATABRICKS_TOKEN"'

        if ($content -ne $originalContent) {
            Write-Host "  -> Changes made to $($file.Name)"
            Set-Content -Path $file.FullName -Value $content -Encoding UTF8
        }
    }
}

# Run the aggressive cleaning
Clean-Secrets-Aggressive -Directory "."

Write-Host "✅ Aggressive secret cleaning completed!"