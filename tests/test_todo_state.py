from backends.todo_state import TodoStore, normalize_full_list


def test_normalize_full_list_accepts_status_aliases():
    todos = normalize_full_list(
        [
            {"content": "A", "status": "done"},
            {"content": "B", "status": "running"},
            {"content": "C", "status": "todo"},
        ]
    )

    assert [todo["status"] for todo in todos] == ["completed", "in_progress", "pending"]


def test_task_create_id_fallback_and_update_status_alias():
    store = TodoStore()

    assert store.note_create("tu1", {"subject": "Finish aliases"})
    assert store.resolve_create("tu1", "created id: 12")
    assert store.apply_update({"taskId": 12, "status": "complete"})

    assert store.as_list() == [
        {
            "id": "12",
            "content": "Finish aliases",
            "status": "completed",
            "activeForm": None,
        }
    ]
