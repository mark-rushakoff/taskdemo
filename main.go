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

	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/platform"
	phttp "github.com/influxdata/platform/http"
	"github.com/influxdata/platform/query"
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

	flag.Usage = func() {
		base := path.Base(os.Args[0])
		out := flag.CommandLine.Output()
		fmt.Fprintf(out, "Usage: %s namespace [bootstrap|list|write|read-in|read-out|destroy]\n", base)
		fmt.Fprintf(out, "\tbootstrap: create org, user, buckets, and authorizations using the given namespace\n")
		fmt.Fprintf(out, "\tlist: list the entities created for the namespace\n")
		fmt.Fprintf(out, "\twrite: write to the input bucket forever\n")
		fmt.Fprintf(out, "\tread-in: read recent data from the input bucket\n")
		fmt.Fprintf(out, "\tread-out: read recent data from the output bucket\n")
		fmt.Fprintf(out, "\tdownsample-once: manually downsample once from the input bucket to the output bucket\n")
		fmt.Fprintf(out, "\tcreate-task: create a task that downsamples from the input bucket to the output bucket\n")
		fmt.Fprintf(out, "\tdestroy: destroy everything in the namespace\n")

		fmt.Fprintf(out, "Typical workflow:\n")
		fmt.Fprintf(out, "\t1. run `bootstrap`\n")
		fmt.Fprintf(out, "\t2. run `write` in another window and leave it running\n")
		fmt.Fprintf(out, "\t3. run `read-in` to check writes to the input bucket\n")
		fmt.Fprintf(out, "\t4. run `downsample-once` to put a single write to the output bucket\n")
		fmt.Fprintf(out, "\t5. run `read-out` to confirm the previous write to the output bucket\n")
		fmt.Fprintf(out, "\t6. run `create-task` to make a task that continually downsamples to the output bucket\n")
	}

	if bootstrapToken == "" {
		flag.Usage()
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
	case "read-in":
		readOnce(bucketInName(), "-5s")
	case "read-out":
		readOnce(bucketOutName(), "-15s")
	case "downsample-once":
		downsampleOnce("-5s")
	case "create-task":
		createTask()
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
		return platform.InvalidID(), err
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
		return platform.InvalidID(), err
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

func bucketOutName() string {
	return "demo-bucket-out-" + namespace
}

func bucketID(ctx context.Context, name string) (platform.ID, error) {
	on := orgName()
	b, err := buckets.FindBucket(ctx, platform.BucketFilter{Name: &name, Organization: &on})
	if err != nil {
		return platform.InvalidID(), err
	}
	return b.ID, nil
}
func mustBucketID(ctx context.Context, name string) platform.ID {
	id, err := bucketID(ctx, name)
	if err != nil {
		log.Fatalf("Failed to find bucket %q: %v", name, err)
	}
	return id
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
		log.Fatalf("Failed to create authorization to write to %s: %v", bIn.Name, err)
	}
	log.Printf("Created authorization to write to bucket %s", bIn.Name)

	authReadIn := &platform.Authorization{
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.ReadBucketPermission(bIn.ID),
		},
	}
	if err := auths.CreateAuthorization(ctx, authReadIn); err != nil {
		log.Fatalf("Failed to create authorization to read from %s: %v", bIn.Name, err)
	}
	log.Printf("Created authorization to read from bucket %s", bIn.Name)

	authReadInWriteOutCreateTask := &platform.Authorization{
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.ReadBucketPermission(bIn.ID),
			platform.WriteBucketPermission(bOut.ID),
			platform.Permission{Action: platform.CreateAction, Resource: platform.TaskResource(o.ID)},
		},
	}
	if err := auths.CreateAuthorization(ctx, authReadInWriteOutCreateTask); err != nil {
		log.Fatalf("Failed to create authorization to read from %s and write to %s: %v", bIn.Name, bOut.Name, err)
	}
	log.Printf("Created authorization to read from bucket %s, write to bucket %s, and create tasks in org %q", bIn.Name, bOut.Name, o.Name)

	authReadOut := &platform.Authorization{
		UserID: u.ID,
		Permissions: []platform.Permission{
			platform.ReadBucketPermission(bOut.ID),
		},
	}
	if err := auths.CreateAuthorization(ctx, authReadOut); err != nil {
		log.Fatalf("Failed to create authorization to read from %s: %v", bOut.Name, err)
	}
	log.Printf("Created authorization to read from bucket %s", bOut.Name)
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

	bInID, err := bucketID(ctx, bucketInName())
	if err == nil {
		log.Printf("Bucket %q with ID %s", bucketInName(), bInID.String())
	} else {
		log.Printf("Could not find bucket %q; continuing...", bucketInName())
	}

	bOutID, err := bucketID(ctx, bucketOutName())
	if err == nil {
		log.Printf("Bucket %q with ID %s", bucketOutName(), bOutID.String())
	} else {
		log.Printf("Could not find bucket %q; continuing...", bucketOutName())
	}

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err == nil {
		for _, a := range as {
			log.Printf("Authorization with ID %s:", a.ID.String())
			log.Printf("\tToken: %s", a.Token)
			for _, p := range a.Permissions {
				log.Printf("\tPermission: action=%s Resource=%s", p.Action, p.Resource)
			}
		}
	} else {
		log.Printf("Could not find authorizations for user %q; continuing...", userName())
	}
}

func write() {
	ctx := context.Background()
	uID := mustUserID(ctx)

	bn := bucketInName()
	on := orgName()
	bInID := mustBucketID(ctx, bn)

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err != nil {
		log.Fatalf("Failed to find authorizations for user with ID %s: %v", uID.String(), err)
	}
	var writeAuth *platform.Authorization
	for _, a := range as {
		if a.Allowed(platform.WriteBucketPermission(bInID)) {
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

func readOnce(bucketName, startRange string) {
	ctx := context.Background()
	oID := mustOrgID(ctx)
	bID := mustBucketID(ctx, bucketName)
	uID := mustUserID(ctx)

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err != nil {
		log.Fatalf("Failed to find authorizations for user with ID %s: %v", uID.String(), err)
	}
	var readAuth *platform.Authorization
	for _, a := range as {
		if a.Allowed(platform.ReadBucketPermission(bID)) {
			readAuth = a
			break
		}
	}
	if readAuth == nil {
		log.Printf("Unable to find existing auth for user %q to read from bucket %q.", userName(), bucketName)
		log.Printf("Found authorizations:")
		for _, a := range as {
			log.Printf("\t%#v\n", a)
		}
		log.Fatalf("Giving up.")
	}

	q := fmt.Sprintf("from(bucket:%q) |> range(start:%s)", bucketName, startRange)
	fqs := phttp.FluxQueryService{Addr: *apiEndpoint, Token: readAuth.Token}
	it, err := fqs.Query(ctx, &query.Request{
		Authorization:  readAuth,
		OrganizationID: oID,

		Compiler: lang.FluxCompiler{Query: q},
	})
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	log.Printf("Executed query: %s", q)

	enc := csv.NewMultiResultEncoder(csv.DefaultEncoderConfig())
	if _, err := enc.Encode(os.Stdout, it); err != nil {
		log.Fatalf("Failed to encode csv: %v", err)
	}
}

func downsampleOnce(startRange string) {
	ctx := context.Background()

	oID := mustOrgID(ctx)
	bInID := mustBucketID(ctx, bucketInName())
	bOutID := mustBucketID(ctx, bucketOutName())
	uID := mustUserID(ctx)
	on := orgName()

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err != nil {
		log.Fatalf("Failed to find authorizations for user with ID %s: %v", uID.String(), err)
	}
	var readWriteAuth *platform.Authorization
	for _, a := range as {
		if a.Allowed(platform.ReadBucketPermission(bInID)) && a.Allowed(platform.WriteBucketPermission(bOutID)) {
			readWriteAuth = a
			break
		}
	}
	if readWriteAuth == nil {
		log.Printf("Unable to find existing auth for user %q to read from bucket %q AND write to bucket %q.", userName(), bucketInName(), bucketOutName())
		log.Printf("Found authorizations:")
		for _, a := range as {
			log.Printf("\t%#v\n", a)
		}
		log.Fatalf("Giving up.")
	}

	q := fmt.Sprintf(
		`from(bucket:%q) |> range(start:%s) |> last() |> to(bucket:%q, org:%q) |> yield()`,
		bucketInName(), startRange, bucketOutName(), on,
	)

	fqs := phttp.FluxQueryService{Addr: *apiEndpoint, Token: readWriteAuth.Token}
	it, err := fqs.Query(ctx, &query.Request{
		Authorization:  readWriteAuth,
		OrganizationID: oID,

		Compiler: lang.FluxCompiler{Query: q},
	})
	if err != nil {
		log.Fatalf("Failed to query: %v", err)
	}
	log.Printf("Executed query: %s", q)

	enc := csv.NewMultiResultEncoder(csv.DefaultEncoderConfig())
	if _, err := enc.Encode(os.Stdout, it); err != nil {
		log.Fatalf("Failed to encode csv: %v", err)
	}
}

func createTask() {
	ctx := context.Background()

	oID := mustOrgID(ctx)
	bInID := mustBucketID(ctx, bucketInName())
	bOutID := mustBucketID(ctx, bucketOutName())
	uID := mustUserID(ctx)

	as, _, err := auths.FindAuthorizations(ctx, platform.AuthorizationFilter{
		UserID: &uID,
	})
	if err != nil {
		log.Fatalf("Failed to find authorizations for user with ID %s: %v", uID.String(), err)
	}
	var readWriteCreateAuth *platform.Authorization
	for _, a := range as {
		if a.Allowed(platform.ReadBucketPermission(bInID)) && a.Allowed(platform.WriteBucketPermission(bOutID)) &&
			a.Allowed(platform.Permission{Action: platform.CreateAction, Resource: platform.TaskResource(oID)}) {
			readWriteCreateAuth = a
			break
		}
	}

	if readWriteCreateAuth == nil {
		log.Printf("Unable to find existing auth for user %q to read from bucket %q AND write to bucket %q AND create tasks in org %s", userName(), bucketInName(), bucketOutName(), oID)
		log.Printf("Found authorizations:")
		for _, a := range as {
			log.Printf("\t%#v\n", a)
		}
		log.Fatalf("Giving up.")
	}

	taskName := fmt.Sprintf("demo-%d", time.Now().Unix())
	f := fmt.Sprintf(
		`option task = { name: %q, every: 5s } from(bucket:%q) |> range(start:-5s) |> last() |> to(bucket:%q, org:%q) |> yield()`,
		taskName, bucketInName(), bucketOutName(), orgName(),
	)

	ts := phttp.TaskService{Addr: *apiEndpoint, Token: readWriteCreateAuth.Token}
	t := &platform.Task{
		Organization: oID,
		Owner: platform.User{
			ID: uID,
		},
		Flux: f,
	}
	if err := ts.CreateTask(ctx, t); err != nil {
		log.Fatalf("Failed to create task: %v", err)
	}

	log.Printf("Created task with ID %s", t.ID)
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
