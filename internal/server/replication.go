package server

type ReplicationState struct {
	role             string
	masterReplid     string
	masterReplOffset int
	followerCount    int
}

func (r *ReplicationState) Get(key string) (string, bool) {
	switch key {
	case "role":
		return r.role, true
	case "masterReplId":
		return r.masterReplid, true
	default:
		return "", false
	}
}

func (r *ReplicationState) GetInt(key string) (int, bool) {
	switch key {
	case "masterReplOffset":
		return r.masterReplOffset, true
	case "followerCount":
		return r.followerCount, true
	default:
		return 0, false
	}
}
