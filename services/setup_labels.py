OFFICEFLOW_LABELS = [
    "OfficeFlow/To Respond",
    "OfficeFlow/FYI",
    "OfficeFlow/Notification",
    "OfficeFlow/Marketing",
]


def setup_gmail_labels_for_tenant(
    *,
    user_id: str,
    tenant_id: str,
    supabase,
    gmail_get_json_for_user,
    gmail_post_json_for_user,
):
    existing = gmail_get_json_for_user(
        user_id=user_id,
        path="/gmail/v1/users/me/labels"
    )

    existing_map = {
        label["name"]: label["id"]
        for label in existing.get("labels", [])
    }

    results = []

    for label_name in OFFICEFLOW_LABELS:
        if label_name in existing_map:
            label_id = existing_map[label_name]
        else:
            created = gmail_post_json_for_user(
                user_id=user_id,
                path="/gmail/v1/users/me/labels",
                payload={
                    "name": label_name,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            )
            label_id = created["id"]

        supabase.table("gmail_labels").upsert(
            {
                "tenant_id": tenant_id,
                "label_name": label_name,
                "label_id": label_id,
            },
            on_conflict="tenant_id,label_name",
        ).execute()

        results.append({
            "label_name": label_name,
            "label_id": label_id,
        })

    return results