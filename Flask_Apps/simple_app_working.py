# Simple Flask App for Databricks Apps - Single File
from flask import Flask, render_template_string, request, jsonify
import pandas as pd
from datetime import datetime
import os

app = Flask(__name__)

# Try to import RAG dependencies
try:
    from databricks.vector_search.client import VectorSearchClient
    import mlflow.deployments
    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False

# Configuration
VECTOR_INDEX_NAME = "network_fault_detection.processed_data.rca_reports_vector_index"
VECTOR_SEARCH_ENDPOINT = "network_fault_detection_vs_endpoint"

class SimpleRAGSystem:
    def __init__(self):
        self.connected = False
        if RAG_AVAILABLE:
            try:
                self.vs_client = VectorSearchClient(disable_notice=True)
                self.index = self.vs_client.get_index(
                    endpoint_name=VECTOR_SEARCH_ENDPOINT,
                    index_name=VECTOR_INDEX_NAME
                )
                self.llm_client = mlflow.deployments.get_deploy_client("databricks")
                self.connected = True
                self.status = "✅ Connected to Real RAG System"
            except Exception as e:
                self.connected = False
                self.status = f"❌ RAG Error: {str(e)[:100]}"
        else:
            self.connected = False
            self.status = "❌ RAG Dependencies Not Available"

    def search(self, query):
        if not self.connected:
            return {
                'response': f"Demo response for: {query}\n\nRAG system not available. Contact admin to install dependencies.",
                'documents': [],
                'status': self.status
            }

        try:
            results = self.index.similarity_search(
                query_text=query,
                columns=["id", "search_content", "incident_priority"],
                num_results=3
            )

            documents = []
            if isinstance(results, dict) and 'result' in results:
                data_array = results['result'].get('data_array', [])
                for i, doc in enumerate(data_array):
                    if isinstance(doc, list) and len(doc) > 0:
                        documents.append({
                            'id': doc[0],
                            'content': doc[1] if len(doc) > 1 else 'No content',
                            'priority': doc[2] if len(doc) > 2 else 'Medium'
                        })

            prompt = f"Network troubleshooting query: {query}\nProvide brief guidance:"
            response = self.llm_client.predict(
                endpoint="databricks-meta-llama-3-1-8b-instruct",
                inputs={"messages": [{"role": "user", "content": prompt}], "temperature": 0.1}
            )

            ai_response = response.get('choices', [{}])[0].get('message', {}).get('content', 'AI response failed')

            return {
                'response': ai_response,
                'documents': documents,
                'status': f"✅ Real RAG Active - {len(documents)} matches"
            }

        except Exception as e:
            return {
                'response': f"RAG error: {e}",
                'documents': [],
                'status': "❌ RAG Failed"
            }

rag = SimpleRAGSystem()

@app.route('/')
def index():
    return render_template_string('''
<!DOCTYPE html>
<html>
<head><title>Network RCA Assistant</title></head>
<body style="font-family: Arial; padding: 20px;">
    <h1>🌐 Network RCA Assistant</h1>
    <p><strong>Status:</strong> {{ status }}</p>
    <form method="POST">
        <textarea name="query" style="width: 100%; height: 100px;" placeholder="Enter network issue...">{{ query or '' }}</textarea><br><br>
        <button type="submit">🚀 Ask RAG System</button>
    </form>
    {% if response %}
    <div style="background: #f0f8ff; padding: 15px; margin: 20px 0; border-left: 4px solid #007bff;">
        <h3>🤖 Response:</h3>
        <pre style="white-space: pre-wrap;">{{ response }}</pre>
    </div>
    {% endif %}
    {% if documents %}
    <div style="background: #f8f9fa; padding: 15px; margin: 20px 0;">
        <h3>📚 Documents Found ({{ documents|length }}):</h3>
        {% for doc in documents %}
        <div style="border: 1px solid #ddd; padding: 10px; margin: 5px 0;">
            <strong>{{ doc.id }}</strong> - {{ doc.priority }}<br>
            {{ doc.content[:200] }}...
        </div>
        {% endfor %}
    </div>
    {% endif %}
</body>
</html>
    ''', status=rag.status, query=request.args.get('query'), response=None, documents=None)

@app.route('/', methods=['POST'])
def process():
    query = request.form.get('query', '').strip()
    if query:
        result = rag.search(query)
        return render_template_string('''
<!DOCTYPE html>
<html>
<head><title>Network RCA Assistant</title></head>
<body style="font-family: Arial; padding: 20px;">
    <h1>🌐 Network RCA Assistant</h1>
    <p><strong>Status:</strong> {{ status }}</p>
    <form method="POST">
        <textarea name="query" style="width: 100%; height: 100px;" placeholder="Enter network issue...">{{ query }}</textarea><br><br>
        <button type="submit">🚀 Ask RAG System</button>
    </form>
    <div style="background: #f0f8ff; padding: 15px; margin: 20px 0; border-left: 4px solid #007bff;">
        <h3>🤖 Response:</h3>
        <pre style="white-space: pre-wrap;">{{ response }}</pre>
    </div>
    {% if documents %}
    <div style="background: #f8f9fa; padding: 15px; margin: 20px 0;">
        <h3>📚 Documents Found ({{ documents|length }}):</h3>
        {% for doc in documents %}
        <div style="border: 1px solid #ddd; padding: 10px; margin: 5px 0;">
            <strong>{{ doc.id }}</strong> - {{ doc.priority }}<br>
            {{ doc.content[:200] }}...
        </div>
        {% endfor %}
    </div>
    {% endif %}
</body>
</html>
        ''', status=result['status'], query=query, response=result['response'], documents=result['documents'])
    return index()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)