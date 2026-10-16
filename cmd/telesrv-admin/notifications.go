package main

import (
	"net/http"
	"strings"

	"telesrv/internal/admin"
)

// sendNotificationAPIRequest mirrors the admin API body. `confirm` follows the
// dashboard's convention used by every other write action: an operator fills the
// form, previews it as a dry run, then ticks confirm to actually send.
//
// That two-step matters more here than for a price change. A notification is
// addressed to a person and cannot be unsent, so the dry run is the only place
// to catch a wrong user id before the recipient has already read it.
type sendNotificationAPIRequest struct {
	CommandID string `json:"command_id"`
	Reason    string `json:"reason"`
	Confirm   bool   `json:"confirm"`
	UserID    int64  `json:"user_id"`
	Text      string `json:"text"`
}

func (s *server) handleSendNotificationAPI(w http.ResponseWriter, r *http.Request) {
	var body sendNotificationAPIRequest
	if !decodeAction(w, r, &body) {
		return
	}
	req := admin.SendNotificationRequest{
		CommandMeta: s.commandMetaFromAPI(r, body.CommandID, body.Reason, body.Confirm, "notification"),
		UserID:      body.UserID,
		Text:        body.Text,
	}
	result, err := s.callAdminAPI(r.Context(), "/v1/notifications/send", req)
	writeCommandResultAPI(w, result, err)
}

// handleNotificationLookupAPI resolves what the operator typed into a single
// account so the recipient can be picked by username rather than by hunting for
// a numeric id.
//
// The recipient is the field most likely to be wrong, and a wrong id delivers a
// moderation notice to a bystander who cannot unsend it. SearchAccounts already
// accepts an id, a username or a phone, so the same box works for all three.
func (s *server) handleNotificationLookupAPI(w http.ResponseWriter, r *http.Request) {
	if s.read == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "read store is not configured")
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		writeAPIError(w, http.StatusBadRequest, "q is required")
		return
	}
	rows, err := s.read.SearchAccounts(r.Context(), query)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(rows) > 10 {
		rows = rows[:10]
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"id":       row.ID,
			"username": row.Username,
			"name":     strings.TrimSpace(row.FirstName + " " + row.LastName),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"rows": out})
}
