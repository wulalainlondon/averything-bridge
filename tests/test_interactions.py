import asyncio

from interactions import PendingInteractionsRegistry, normalize_questions


def test_normalize_questions_supports_choices_and_recommended_option():
    questions = normalize_questions({
        "questions": [{
            "id": "install",
            "question": "Install APK?",
            "options": [
                {"label": "Yes", "description": "Install now", "recommended": True},
                {"label": "No"},
            ],
        }]
    })

    assert questions == [{
        "question_id": "install",
        "text": "Install APK?",
        "header": "",
        "type": "choice",
        "options": [
            {"id": "Yes", "label": "Yes", "description": "Install now", "recommended": True},
            {"id": "No", "label": "No", "description": "", "recommended": False},
        ],
        "multi_select": False,
        "free_form": False,
    }]


def test_registry_create_list_resolve_and_broadcast():
    async def run():
        registry = PendingInteractionsRegistry()
        broadcasts = []

        async def broadcast(payload):
            broadcasts.append(payload)
            return 1

        item = await registry.create(
            session_id="s1",
            source="claude",
            kind="ask_user_question",
            questions=normalize_questions({"question": "Continue?"}),
            tool_use_id="tool_1",
            broadcast_json=broadcast,
        )
        pending = await registry.list_pending()
        resolved = await registry.resolve(
            {"type": "user_input_response", "request_id": "tool_1", "answers": {"q1": "yes"}},
            broadcast_json=broadcast,
        )
        remaining = await registry.list_pending()
        return item, pending, resolved, remaining, broadcasts

    item, pending, resolved, remaining, broadcasts = asyncio.run(run())

    assert pending[0]["request_id"] == item.request_id
    assert resolved and resolved.request_id == item.request_id
    assert remaining == []
    assert broadcasts[0]["type"] == "user_input_request"
    assert broadcasts[-1]["type"] == "interaction_resolved"
