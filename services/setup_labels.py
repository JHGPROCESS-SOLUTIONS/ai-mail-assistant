# DEPRECATED — DO NOT USE
#
# This module previously created only 4 Gmail labels (To Respond, FYI,
# Notification, Marketing) and is missing the rest of the canonical label
# set (Priority, Waiting On Reply, Follow Up, Done, Ignore).
#
# The canonical, complete label setup lives in app/main.py:
#   - LABELS                       — list of all 9 status labels
#   - setup_gmail_labels_for_mailbox()  — creates every label for a mailbox
#   - LEGACY_LABEL_NAME_MAP        — handles "OfficeFlow/<name>" migration
#
# Importing from this file now raises so we never accidentally regress to
# an incomplete setup. Delete this file via your Git client when you can.

raise ImportError(
    "services/setup_labels.py is deprecated. Use "
    "app.main.setup_gmail_labels_for_mailbox() — it creates the full "
    "9-label canonical set, not just 4."
)
