package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/influxdata/platform"
	phttp "github.com/influxdata/platform/http"
)

var (
	apiEndpoint = flag.String("api", "http://localhost:9999", "HTTP endpoint of API server")

	bootstrapToken = os.Getenv("BOOTSTRAP_TOKEN")

	namespace string
	users     phttp.UserService
	orgs      phttp.OrganizationService
	buckets   phttp.BucketService
	auths     phttp.AuthorizationService
)

func init() {
	log.SetFlags(log.Ltime | log.Lshortfile | log.Lmicroseconds)

	if bootstrapToken == "" {
		log.Fatalf("Environment variable BOOTSTRAP_TOKEN must be set to do anything with this demo.")
	}
}

func initServices() {
	a := *apiEndpoint
	t := bootstrapToken

	users = phttp.UserService{Addr: a, Token: t}
	orgs = phttp.OrganizationService{Addr: a, Token: t}
	buckets = phttp.BucketService{Addr: a, Token: t}
	auths = phttp.AuthorizationService{Addr: a, Token: t}
}

func main() {
	flag.Usage = func() {
		base := path.Base(os.Args[0])
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "Usage: %s namespace [bootstrap|list|write|destroy]\n", base)
		fmt.Fprintf(out, "\tbootstrap: create org, user, buckets, and authorizations using the given namespace\n")
		fmt.Fprintf(out, "\tlist: list the entities created for the namespace\n")
		fmt.Fprintf(out, "\twrite: write to the input bucket forever\n")
		fmt.Fprintf(out, "\tdestroy: destroy everything in the namespace\n")

		fmt.Fprintf(out, "Typical workflow:\n")
		fmt.Fprintf(out, "\t1. run `bootstrap`\n")
		fmt.Fprintf(out, "\t2. run `write` in another window and leave it running\n")
	}

	flag.Parse()
	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}
	namespace = flag.Arg(0)

	initServices()

	switch flag.Arg(1) {
	case "bootstrap":
		bootstrap()
	case "list":
		list()
	case "write":
		write()
	case "destroy":
		destroy()
	default:
		flag.Usage()
		os.Exit(1)
	}
	os.Exit(0)
}

func userName() string {
	return "demo-user-" + namespace
}
func userID(ctx context.Context) (platform.ID, error) {
	un := userName()
	u, err := users.FindUser(ctx, platform.UserFilter{Name: &un})
	if err != nil {
		return nil, err
	}
	return u.ID, nil
}
func mustUserID(ctx context.Context) platform.ID {
	uid, err := userID(ctx)
	if err != nil {
		log.Fatalf("Failed to find user %q: %v", userName(), err)
	}
	return uid
}

func orgName() string {
	return "demo-org-" + namespace
}
func orgID(ctx context.Context) (platform.ID, error) {
	on := orgName()
	o, err := orgs.FindOrganization(context.Background(), platform.OrganizationFilter{Name: &on})
	if err != nil {
		return nil, err
	}
	return o.ID, nil
}
func mustOrgID(ctx context.Context) platform.ID {
	oid, err := orgID(ctx)
	if err != nil {
		log.Fatalf("Failed to find org %q: %v", orgName(), err)
	}
	return oid
}

func bucketInName() string {
	return "demo-bucket-in-" + namespace
}
func bucketInID(ctx context.Context) (platform.ID, error) {
	bn := bucketInName()
	on := orgName()
	bIn, err := buckets.FindBucket(ctx, platform.BucketFilter{Name: &bn, Organization: &on})
	if err != nil {
		return nil, err
	}
	return bIn.ID, nil
}
func mustBucketInID(ctx context.Context) platform.ID {
	bInID, err := bucketInID(ctx)
	if err != nil {
		log.Fatalf("Failed to find bucket %q: %v", bucketInName, err)
	}
	return bInID
}

func bucketOutName() string {
	return "demo-bucket-out-" + namespace
}
func bucketOutID(ctx context.Context) (platform.ID, error) {
	bn := bucketOutName()
	on := orgName()
	bOut, err := buckets.FindBucket(ctx, platform.BucketFilter{Name: &bn, Organization: &on})
	if err != nil {
		return nil, err
	}
	return bOut.ID, nil
}
func mustBucketOutID(ctx context.Context) platform.ID {
	bOutID, err := bucketOutID(ctx)
	if err != nil {
		log.Fatalf("Failed to find bucket %q: %v", bucketOutName, err)
	}
	return bOutID
}

func bootstrap() {
	ctx := context.Background()

	u := &platform.User{Name: userName()}
	if err := users.CreateUser(ctx, u); err != nil {
		log.Fatalf("Failed to create user: %v", err)
	}
	log.Printf("Created user %q with ID %s", u.Name, u.ID.String())

	o := &platform.Organization{Name: orgName()}
	if err := orgs.CreateOrganization(ctx, o); err != nil {
		log.Fatalf("Failed to create org: %v", err)
	}
	log.Printf("Created org %q with ID %s", o.Name, o.ID.String())

	bIn := &platform.Bucket{Name: bucketInName(), OrganizationID: o.ID, RetentionPeriod: time.Hour}
	if err := buckets.CreateBucket(ctx, bIn); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	log.Printf("Created bucket %q with ID %s", bIn.Name, bIn.ID.String())
	bOut := &platform.Bucket{Name: bucketOutName(), OrganizationID: o.ID, RetentionPeriod: 24 * time.Hour}
	if err := buckets.CreateBucket(ctx, bOut); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	log.Printf("Created bucket %q with ID %s", bOut.Name, bOut.ID.String())

	authWriteIn := &platform.Authorization{
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.WriteBucketPermission(bIn.ID),
		},
	}
	if err := auths.CreateAuthorization(ctx, authWriteIn); err != nil {
		log.Fatalf("Failed to create authorization to write to %s", bIn.Name)
	}
}

func list() {
	ctx := context.Background()

	uID, err := userID(ctx)
	if err == nil {
		log.Printf("User %q with ID %s", userName(), uID.String())
	} else {
		log.Printf("Could not find user %q; continuing...", userName())
	}

	oID, err := orgID(ctx)
	if err == nil {
		log.Printf("Org %q with ID %s", orgName(), oID.String())
	} else {
		log.Printf("Could not find org %q; continuing...", orgName())
	}

	bInID, err := bucketInID(ctx)
	if err == nil {
		log.Printf("Bucket %q with ID %s", bucketInName(), bInID.String())
	} else {
		log.Printf("Could not find bucket %q; continuing...", bucketInName())
	}

	bOutID, err := bucketOutID(ctx)
	if err == nil {
		log.Printf("Bucket %q with ID %s", bucketOutName(), bOutID.String())
	} else {
		log.Printf("Could not find bucket %q; continuing...", bucketOutName())
	}
}

func write() {
	ctx := context.Background()
	uID := mustUserID(ctx)

	bn := bucketInName()
	on := orgName()
	bIn, err := buckets.FindBucket(ctx, platform.BucketFilter{Name: &bn, Organization: &on})
	if err != nil {
		log.Fatalf("Failed to find bucket %q: %v", bn, err)
	}

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err != nil {
		log.Fatalf("Failed to find authorizations for user with ID %s: %v", bn, err)
	}
	var writeAuth *platform.Authorization
	for _, a := range as {
		if a.Allowed(platform.WriteBucketPermission(bIn.ID)) {
			writeAuth = a
			break
		}
	}
	if writeAuth == nil {
		log.Printf("Unable to find existing auth for user %q to write to bucket %q.", userName(), bn)
		log.Printf("Found authorizations:")
		for _, a := range as {
			log.Printf("\t%v\n", a)
		}
		log.Fatalf("Giving up.")
	}

	url := *apiEndpoint + "/api/v2/write?org=" + url.QueryEscape(on) + "&bucket=" + url.QueryEscape(bn)

	for i := 0; ; i++ {
		if i != 0 {
			time.Sleep(100 * time.Millisecond)
		}
		write := fmt.Sprintf("counter n=%d", i)
		req, err := http.NewRequest("POST", url, strings.NewReader(write))
		if err != nil {
			log.Fatalf("Failed to create request: %v", err)
		}
		req.Header.Set("User-Agent", "demo")
		phttp.SetToken(writeAuth.Token, req)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Fatalf("Failed to write batch: %v", err)
		}
		if resp.StatusCode != 204 {
			log.Fatalf("Unexpected response status code from write: %d", resp.StatusCode)
		}

		log.Printf("Successfully wrote %q to bucket %q in org %q", write, bn, on)
	}
}

func destroy() {
	ctx := context.Background()

	uID, err := userID(ctx)
	if err == nil {
		if err := users.DeleteUser(ctx, uID); err == nil {
			log.Printf("Deleted user %q", userName())
		} else {
			log.Printf("Failed to delete user with ID %s: %v", uID.String(), err)
		}
	} else {
		log.Printf("Could not find user %q; continuing...", userName())
	}

	oID, err := orgID(ctx)
	if err == nil {
		if err := orgs.DeleteOrganization(ctx, oID); err == nil {
			log.Printf("Deleted org %q", orgName())
		} else {
			log.Printf("Failed to delete org with ID %s: %v", oID.String(), err)
		}
	} else {
		log.Printf("Could not find org %q; continuing...", orgName())
	}
}
