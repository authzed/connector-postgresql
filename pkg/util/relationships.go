package util

import (
	"fmt"

	"github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// RelString best-effort formats a relationship for debug logging
func RelString(r *v1.Relationship) string {
	if r == nil {
		return ""
	}
	if r.Resource == nil {
		r.Resource = &v1.ObjectReference{}
	}
	if r.Subject == nil {
		r.Subject = &v1.SubjectReference{}
	}
	if r.Subject.Object == nil {
		r.Subject.Object = &v1.ObjectReference{}
	}
	return fmt.Sprintf("%s:%s#%s@%s:%s", r.Resource.ObjectType, r.Resource.ObjectId, r.Relation, r.Subject.Object.ObjectType, r.Subject.Object.ObjectId)
}
