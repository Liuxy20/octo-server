//go:build integration

package message

// Post-fix end-to-end coverage for issue #484 — DM per-Space isolation.
//
// Originally a reproduction (DMs leaked across Spaces / mutually hid between
// Spaces); now flipped to assert the fix, driven through the REAL registered
// handlers against MySQL/Redis with WuKongIM mocked:
//
//   - Symptom 2 (was: mutual hide): conversation visibility now comes from the
//     authoritative dm_space_presence index, written at the REAL WuKongIM
//     message webhook (POST /v1/webhook/message/notify) and read by
//     /v1/conversation/sync — so a DM is visible in EVERY Space it has messages
//     in, simultaneously, independent of the shared Recents window.
//   - Symptom 1 (was: history leak): /v1/message/channel/sync keeps an untagged
//     DM message only in the user's default Space.
//
// Build-tagged `integration` (NOT compiled in the default CI `go test` job).
// Run targeted (order-fragile like every e2e file here — register.GetModules
// memoizes modules under a sync.Once, so handlers bind to the FIRST
// NewTestServer's ctx/config in the process):
//
//	go test -tags=integration ./modules/message/ -run TestRepro484 -v

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Mininglamp-OSS/octo-lib/common"
	"github.com/Mininglamp-OSS/octo-lib/config"
	"github.com/Mininglamp-OSS/octo-lib/pkg/util"
	"github.com/Mininglamp-OSS/octo-lib/server"
	"github.com/Mininglamp-OSS/octo-lib/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const reproPeerUID = "peer_contact_777"
const reproWebhookSecret = "repro-webhook-secret-484"

// Three Spaces the test user belongs to. spaceDefault is joined first so it is
// the user's default Space (GetUserDefaultSpaceID = earliest created_at). spaceB
// and spaceC are both NON-default, which isolates the per-Space behaviour from
// the "default Space always shows" special-case.
const (
	reproSpaceDefault = "spaceDefault"
	reproSpaceB       = "spaceB"
	reproSpaceC       = "spaceC"
)

// One long-lived fake WuKongIM for this file, serving both IM endpoints the two
// handlers hit, off mutable response vars. Tests run sequentially (no
// t.Parallel), so the shared vars need no lock.
var (
	reproIMOnce sync.Once
	reproIMSrv  *httptest.Server
	reproIMConv *config.SyncUserConversationResp // /conversation/sync payload (single DM)
	reproIMMsgs []*config.MessageResp            // /channel/messagesync payload
)

func reproFakeIM() *httptest.Server {
	reproIMOnce.Do(func() {
		reproIMSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case strings.HasSuffix(r.URL.Path, "/conversation/sync"):
				convs := []*config.SyncUserConversationResp{}
				if reproIMConv != nil {
					convs = append(convs, reproIMConv)
				}
				_, _ = w.Write([]byte(util.ToJson(convs)))
			case strings.HasSuffix(r.URL.Path, "/channel/messagesync"):
				_, _ = w.Write([]byte(util.ToJson(&config.SyncChannelMessageResp{
					StartMessageSeq: 1,
					EndMessageSeq:   uint32(len(reproIMMsgs)),
					Messages:        reproIMMsgs,
				})))
			default:
				_, _ = w.Write([]byte("{}"))
			}
		}))
	})
	return reproIMSrv
}

// reproMsg builds an IM message with the given content marker and (optional)
// payload.space_id tag. spaceID=="" → an UNTAGGED message.
func reproMsg(seq uint32, content, spaceID string) *config.MessageResp {
	payload := map[string]interface{}{"type": 1, "content": content}
	if spaceID != "" {
		payload["space_id"] = spaceID
	}
	return &config.MessageResp{
		MessageID:   int64(seq),
		MessageSeq:  seq,
		ClientMsgNo: content,
		FromUID:     reproPeerUID,
		ChannelID:   reproPeerUID,
		ChannelType: common.ChannelTypePerson.Uint8(),
		Timestamp:   1700000000 + int32(seq),
		IsDeleted:   0,
		Payload:     []byte(util.ToJson(payload)),
	}
}

// reproDMConv wraps a single UNTAGGED recent message into a DM conversation as
// IMSyncUserConversation would return it. Untagged Recents means the legacy
// window-scan OR-term never matches spaceB/spaceC, so conv visibility in those
// Spaces is driven purely by the authoritative dm_space_presence index.
func reproDMConv() *config.SyncUserConversationResp {
	return &config.SyncUserConversationResp{
		ChannelID:   reproPeerUID,
		ChannelType: common.ChannelTypePerson.Uint8(),
		Unread:      0,
		Timestamp:   1700000099,
		LastMsgSeq:  1,
		Version:     100,
		Recents:     []*config.MessageResp{reproMsg(1, "untagged-recent", "")},
	}
}

func reproSeedSpace(t *testing.T, ctx *config.Context, spaceID, createdAt string) {
	t.Helper()
	_, err := ctx.DB().InsertBySql(
		"INSERT INTO space (space_id, name, creator, status, created_at, updated_at) VALUES (?, ?, ?, 1, ?, ?)",
		spaceID, spaceID, testutil.UID, createdAt, createdAt,
	).Exec()
	require.NoError(t, err)
	_, err = ctx.DB().InsertBySql(
		"INSERT INTO space_member (space_id, uid, role, status, created_at, updated_at) VALUES (?, ?, 0, 1, ?, ?)",
		spaceID, testutil.UID, createdAt, createdAt,
	).Exec()
	require.NoError(t, err)
}

// reproSetup wires a fresh test server with the fake IM, seeds the three Space
// memberships, configures the webhook HMAC secret, and clears Redis state so
// each test is deterministic.
func reproSetup(t *testing.T) (*server.Server, *config.Context) {
	t.Helper()

	// module.Setup (common module) refuses to start without a master key.
	t.Setenv("OCTO_MASTER_KEY", "0123456789abcdef0123456789abcdef")
	// Webhook reads its HMAC secret from env at module construction.
	t.Setenv("TS_WEBHOOK_SECRET_KEY", reproWebhookSecret)

	imURL := reproFakeIM().URL
	s, ctx := testutil.NewTestServer()
	require.NoError(t, testutil.CleanAllTables(ctx))

	cfg := ctx.GetConfig()
	cfg.MessageSaveAcrossDevice = false
	cfg.WuKongIM.APIURL = imURL

	require.NoError(t, ctx.Cache().Set(cfg.Cache.TokenCachePrefix+testutil.Token, testutil.UID+"@test"))

	// spaceDefault joined first → user's default Space. B and C are non-default.
	reproSeedSpace(t, ctx, reproSpaceDefault, "2020-01-01 00:00:00")
	reproSeedSpace(t, ctx, reproSpaceB, "2021-01-01 00:00:00")
	reproSeedSpace(t, ctx, reproSpaceC, "2022-01-01 00:00:00")

	r := ctx.GetRedisConn()
	_ = r.Del("ratelimit:uid:" + testutil.UID)
	_ = r.Del("userMaxVersion:" + testutil.UID)
	for _, sp := range []string{reproSpaceDefault, reproSpaceB, reproSpaceC} {
		_ = r.Del("space:member:" + sp + ":" + testutil.UID)
	}
	return s, ctx
}

// reproIngestDM drives the REAL WuKongIM message webhook for an inbound DM
// (peer → login user) tagged with spaceID, populating dm_space_presence. The
// HMAC signature satisfies verifyRequestSignature. Payload is a []byte JSON
// field → base64-encoded on the wire (encoding/json round-trips []byte).
func reproIngestDM(t *testing.T, s *server.Server, spaceID string, seq uint32) {
	t.Helper()
	payload := fmt.Sprintf(`{"type":1,"content":"m%d","space_id":%q}`, seq, spaceID)
	b64 := base64.StdEncoding.EncodeToString([]byte(payload))
	// from_uid = peer, channel_id = login user → GetFakeChannelIDWith canonical
	// pair matches the conversation read side GetFakeChannelIDWith(loginUID, peer).
	body := fmt.Sprintf(
		`[{"message_id":%d,"message_seq":%d,"from_uid":%q,"channel_id":%q,"channel_type":1,"timestamp":%d,"payload":%q}]`,
		seq, seq, reproPeerUID, testutil.UID, 1700000000+int(seq), b64,
	)
	mac := hmac.New(sha256.New, []byte(reproWebhookSecret))
	mac.Write([]byte(body))
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/webhook/message/notify", strings.NewReader(body))
	req.Header.Set("X-Signature-256", sig)
	s.GetRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

// reproCallConvSync drives POST /v1/conversation/sync with X-Space-ID and
// returns the channel IDs present in the response conversation list.
func reproCallConvSync(t *testing.T, s *server.Server, spaceID string) []string {
	t.Helper()
	body := `{"version":0,"msg_count":50,"device_uuid":"dev-repro","recent_filter":false}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/conversation/sync", strings.NewReader(body))
	req.Header.Set("token", testutil.Token)
	req.Header.Set("X-Space-ID", spaceID)
	s.GetRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var wrap struct {
		Conversations []struct {
			ChannelID string `json:"channel_id"`
		} `json:"conversations"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &wrap))
	ids := make([]string, 0, len(wrap.Conversations))
	for _, c := range wrap.Conversations {
		ids = append(ids, c.ChannelID)
	}
	return ids
}

// reproCallChannelSync drives POST /v1/message/channel/sync for the DM channel
// with X-Space-ID and returns the content markers of the returned messages.
func reproCallChannelSync(t *testing.T, s *server.Server, spaceID string) []string {
	t.Helper()
	body := `{"channel_id":"` + reproPeerUID + `","channel_type":1,"limit":100,"pull_mode":1}`
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/v1/message/channel/sync", strings.NewReader(body))
	req.Header.Set("token", testutil.Token)
	req.Header.Set("X-Space-ID", spaceID)
	s.GetRoute().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var resp struct {
		Messages []struct {
			Payload map[string]interface{} `json:"payload"`
		} `json:"messages"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	contents := make([]string, 0, len(resp.Messages))
	for _, m := range resp.Messages {
		if c, ok := m.Payload["content"].(string); ok {
			contents = append(contents, c)
		}
	}
	return contents
}

func reproContains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// TestRepro484_Symptom2_DMVisibleInEverySpaceItHasMessages asserts the fix for
// symptom 2: a single DM that has messages in BOTH spaceB and spaceC is visible
// in BOTH simultaneously — the shared Recents window no longer makes them
// mutually exclusive. Presence is written via the REAL message webhook.
func TestRepro484_Symptom2_DMVisibleInEverySpaceItHasMessages(t *testing.T) {
	s, _ := reproSetup(t)
	reproIMConv = reproDMConv() // Recents are UNTAGGED → visibility is presence-driven

	// The contact was chatted with in spaceB AND spaceC (durable presence rows).
	reproIngestDM(t, s, reproSpaceB, 1)
	reproIngestDM(t, s, reproSpaceC, 2)

	inB := reproCallConvSync(t, s, reproSpaceB)
	inC := reproCallConvSync(t, s, reproSpaceC)

	assert.True(t, reproContains(inB, reproPeerUID),
		"DM visible in spaceB (authoritative presence), independent of the Recents window")
	assert.True(t, reproContains(inC, reproPeerUID),
		"FIXED symptom 2: the SAME DM is also visible in spaceC at the same time — no mutual hide")
}

// TestRepro484_Symptom2_IsolationPreserved asserts we did not over-correct: a DM
// with messages only in spaceB is visible in spaceB but NOT in spaceC (which it
// genuinely has no messages in).
func TestRepro484_Symptom2_IsolationPreserved(t *testing.T) {
	s, _ := reproSetup(t)
	reproIMConv = reproDMConv()

	reproIngestDM(t, s, reproSpaceB, 1) // only spaceB

	inB := reproCallConvSync(t, s, reproSpaceB)
	inC := reproCallConvSync(t, s, reproSpaceC)

	assert.True(t, reproContains(inB, reproPeerUID), "DM visible in the Space it has messages in")
	assert.False(t, reproContains(inC, reproPeerUID),
		"isolation preserved: DM absent from a Space it has no messages in")
}

// TestRepro484_Symptom1_UntaggedHistoryOnlyInDefaultSpace asserts the fix for
// symptom 1: an untagged DM message is kept only in the user's default Space,
// no longer leaking into every Space.
func TestRepro484_Symptom1_UntaggedHistoryOnlyInDefaultSpace(t *testing.T) {
	s, _ := reproSetup(t)

	// One DM history: a spaceB-tagged msg, an UNTAGGED msg, a spaceC-tagged msg.
	reproIMMsgs = []*config.MessageResp{
		reproMsg(1, "msg-tagged-B", reproSpaceB),
		reproMsg(2, "msg-UNTAGGED", ""),
		reproMsg(3, "msg-tagged-C", reproSpaceC),
	}

	// Non-default Space (spaceB): only its own tagged message; untagged dropped.
	inB := reproCallChannelSync(t, s, reproSpaceB)
	assert.True(t, reproContains(inB, "msg-tagged-B"), "spaceB sees its own tagged msg")
	assert.False(t, reproContains(inB, "msg-tagged-C"), "spaceB does NOT see spaceC's tagged msg")
	assert.False(t, reproContains(inB, "msg-UNTAGGED"),
		"FIXED symptom 1: untagged msg no longer leaks into non-default spaceB")

	// Default Space: the untagged message is retained (forward-compat).
	inDefault := reproCallChannelSync(t, s, reproSpaceDefault)
	assert.True(t, reproContains(inDefault, "msg-UNTAGGED"), "untagged msg retained in default Space")
	assert.False(t, reproContains(inDefault, "msg-tagged-B"), "default Space does NOT see spaceB's tagged msg")
}
