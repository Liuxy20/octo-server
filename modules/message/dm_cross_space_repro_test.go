//go:build integration

package message

// Reproduction harness for issue #484 — DMs leak across Spaces (symptom 1) and
// mutually hide between Spaces (symptom 2). Both are driven end-to-end through
// the REAL registered HTTP handlers:
//
//   - symptom 2: POST /v1/conversation/sync  (FilterConversationsBySpace →
//     decideConvKeepInSpace → personConvHasSpaceMessages, space_filter.go)
//   - symptom 1: POST /v1/message/channel/sync (filterPersonMessagesBySpace,
//     space_filter.go:442)
//
// WuKongIM is mocked by a local httptest server (the only external seam the
// handlers can't reach in CI): it answers /conversation/sync and
// /channel/messagesync from mutable package vars. MySQL + Redis must be up.
//
// Build-tagged `integration` (NOT compiled in the default CI `go test` job),
// matching the sibling e2e files. Run targeted:
//
//	go test -tags=integration ./modules/message/ -run TestRepro484 -v
//
// Like every e2e file here it is order-fragile: register.GetModules memoizes
// modules under a sync.Once, so the registered handlers bind to the FIRST
// NewTestServer's ctx/config in the process. Run this file's tests on their own
// (the -run above) so this file's NewTestServer is the one that binds the IM URL.
// Distinct identifiers (repro*) so it still links cleanly with
// conversation_recent_filter_e2e_test.go in the same package.

import (
	"encoding/json"
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

// Three Spaces the test user belongs to. spaceDefault is joined first so it is
// the user's default Space (GetUserDefaultSpaceID = earliest created_at). spaceB
// and spaceC are both NON-default, which isolates symptom 2 from the
// "default Space always shows" special-case.
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
	reproIMConv *config.SyncUserConversationResp     // /conversation/sync payload (single DM)
	reproIMMsgs []*config.MessageResp                // /channel/messagesync payload
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
// payload.space_id tag. spaceID=="" → an UNTAGGED message (the exact shape the
// server produces when the sender omits X-Space-ID, and what legacy/forwarded
// messages look like).
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

// reproDMConv wraps the given recent messages into a single-DM conversation as
// IMSyncUserConversation would return it.
func reproDMConv(recents ...*config.MessageResp) *config.SyncUserConversationResp {
	return &config.SyncUserConversationResp{
		ChannelID:   reproPeerUID,
		ChannelType: common.ChannelTypePerson.Uint8(),
		Unread:      0,
		Timestamp:   1700000099,
		LastMsgSeq:  int64(len(recents)),
		Version:     100,
		Recents:     recents,
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
// memberships, and clears Redis state (rate-limit bucket, conversation cursor,
// membership cache) so each test is deterministic.
func reproSetup(t *testing.T) (*server.Server, *config.Context) {
	t.Helper()

	// module.Setup (common module) refuses to start without a master key.
	t.Setenv("OCTO_MASTER_KEY", "0123456789abcdef0123456789abcdef")

	imURL := reproFakeIM().URL
	s, ctx := testutil.NewTestServer()
	require.NoError(t, testutil.CleanAllTables(ctx))

	cfg := ctx.GetConfig()
	cfg.MessageSaveAcrossDevice = false
	cfg.WuKongIM.APIURL = imURL

	// Default normal-user token (uid@name). NewTestServer already set this, but a
	// prior test in the same binary may have overwritten it (e.g. SuperAdmin).
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

// TestRepro484_Symptom2_DMMutuallyHidesBetweenSpaces reproduces symptom 2:
// the SAME single DM conversation is visible in whichever non-default Space last
// produced a message into the shared Recents window, and hidden in the other.
func TestRepro484_Symptom2_DMMutuallyHidesBetweenSpaces(t *testing.T) {
	s, _ := reproSetup(t)

	// Round 1: the contact was last chatted with in spaceB. The single physical
	// DM channel's Recents window therefore carries only spaceB-tagged messages.
	reproIMConv = reproDMConv(reproMsg(1, "hi-from-spaceB", reproSpaceB))

	inB := reproCallConvSync(t, s, reproSpaceB)
	inC := reproCallConvSync(t, s, reproSpaceC)

	assert.True(t, reproContains(inB, reproPeerUID),
		"DM visible in spaceB (its tagged message is in the Recents window)")
	assert.False(t, reproContains(inC, reproPeerUID),
		"SYMPTOM 2: same DM has VANISHED from spaceC — the shared Recents window holds no spaceC-tagged message")

	// Round 2: activity moves to spaceC (the window is shared, so it now holds
	// only spaceC-tagged messages). Visibility flips — proving "most-recently
	// active Space wins; the other hides".
	reproIMConv = reproDMConv(reproMsg(2, "hi-from-spaceC", reproSpaceC))

	inB = reproCallConvSync(t, s, reproSpaceB)
	inC = reproCallConvSync(t, s, reproSpaceC)

	assert.True(t, reproContains(inC, reproPeerUID),
		"DM now visible in spaceC after activity moved there")
	assert.False(t, reproContains(inB, reproPeerUID),
		"SYMPTOM 2 (mirror): the same DM has now VANISHED from spaceB")
}

// TestRepro484_Symptom1_DMHistoryLeaksAcrossSpaces reproduces symptom 1:
// an UNTAGGED message (sender omitted X-Space-ID / legacy / forwarded) survives
// the per-message history filter in EVERY Space, so it leaks into a Space it was
// never meant for, while correctly-tagged messages stay isolated.
func TestRepro484_Symptom1_DMHistoryLeaksAcrossSpaces(t *testing.T) {
	s, _ := reproSetup(t)

	// One DM history with: a spaceB-tagged msg, an UNTAGGED msg, a spaceC-tagged msg.
	reproIMMsgs = []*config.MessageResp{
		reproMsg(1, "msg-tagged-B", reproSpaceB),
		reproMsg(2, "msg-UNTAGGED", ""),
		reproMsg(3, "msg-tagged-C", reproSpaceC),
	}

	inB := reproCallChannelSync(t, s, reproSpaceB)
	inC := reproCallChannelSync(t, s, reproSpaceC)

	// Tagged messages are correctly isolated to their own Space.
	assert.True(t, reproContains(inB, "msg-tagged-B"), "spaceB sees its own tagged msg")
	assert.False(t, reproContains(inB, "msg-tagged-C"), "spaceB does NOT see spaceC's tagged msg")
	assert.True(t, reproContains(inC, "msg-tagged-C"), "spaceC sees its own tagged msg")
	assert.False(t, reproContains(inC, "msg-tagged-B"), "spaceC does NOT see spaceB's tagged msg")

	// SYMPTOM 1: the untagged message leaks into BOTH Spaces (rule 2 of
	// filterPersonMessagesBySpace keeps untagged non-systembot msgs everywhere).
	assert.True(t, reproContains(inB, "msg-UNTAGGED"), "untagged msg appears in spaceB")
	assert.True(t, reproContains(inC, "msg-UNTAGGED"),
		"SYMPTOM 1: the SAME untagged msg also appears in spaceC — cross-Space history leak")
}
