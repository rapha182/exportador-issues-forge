from fastapi import FastAPI, Request
from pydantic import BaseModel
import requests
import pandas as pd
import os

app = FastAPI()

BASE_URL = "https://estaparjsm.atlassian.net/rest/api/3/search"
AUTH = (os.getenv("JIRA_EMAIL"), os.getenv("JIRA_TOKEN"))
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

class JQLRequest(BaseModel):
    jql: str

def get_nested_value(data, path):
    keys = path.split('.')
    current_data = data
    for key in keys:
        if isinstance(current_data, dict) and key in current_data:
            current_data = current_data[key]
        else:
            return 'N/A'
    return current_data if current_data is not None else 'N/A'

@app.post("/export")
def export_issues(body: JQLRequest):
    try:
        jql = body.jql
        max_results = 100
        start_at = 0
        issues_list = []

        while True:
            params = {
                'jql': jql,
                'startAt': start_at,
                'maxResults': max_results,
                'fields': 'project,key,issuetype,status,assignee,reporter,created,resolutiondate,customfield_10680,customfield_10767,customfield_10010',
                'expand': 'changelog'
            }
            response = requests.get(BASE_URL, headers=HEADERS, auth=AUTH, params=params)

            if not response.ok:
                return {
                    "status_code": response.status_code,
                    "error_text": response.text
                }

            data = response.json()

            issues = data.get('issues', [])
            if not issues:
                break
            for issue in issues:
                resolved_date = None
                for history in issue.get('changelog', {}).get('histories', []):
                    for item in history.get('items', []):
                        if item['field'] == 'status' and item['toString'] == 'Resolvido':
                            resolved_date = history['created']
                if resolved_date:
                    issue['fields']['resolvedDate'] = resolved_date
                issues_list.append(issue)
            start_at += len(issues)

        # Montar DataFrame
        df_issues = pd.DataFrame([
            {
                'Projeto': issue['fields']['project']['name'],
                'Issue Key': issue['key'],
                'Issue Type': issue['fields']['issuetype']['name'],
                'Status': issue['fields']['status']['name'],
                'Assignee': issue['fields'].get('assignee', {}).get('displayName', 'Unassigned'),
                'Reporter': issue['fields'].get('reporter', {}).get('displayName', 'Unknown'),
                'Created Date': issue['fields']['created'],
                'Resolved Date': issue['fields'].get('resolvedDate', 'Not resolved'),
                'Tipo de Requisição': get_nested_value(issue['fields'], 'customfield_10680'),
                'Grupo Solucionador': get_nested_value(issue['fields'], 'customfield_10767.value'),
                'Request Type': get_nested_value(issue['fields'], 'customfield_10010.requestType.name'),
            }
            for issue in issues_list
        ])

        # Conversão de datas
        df_issues['Created Date'] = pd.to_datetime(df_issues['Created Date'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
        df_issues['Resolved Date'] = pd.to_datetime(df_issues['Resolved Date'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')

        preview = df_issues.head(5).to_dict(orient="records")
        return {
            "total_issues": len(df_issues),
            "preview": preview
        }
    
    except Exception as e:
        return {"error": str(e)}
