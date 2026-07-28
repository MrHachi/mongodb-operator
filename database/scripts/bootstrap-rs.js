var hostname = process.env.HOSTNAME;
var svcname = process.env.SVC_NAME;
var ns = process.env.NAMESPACE;

var icfg = {
  _id: "rs0",
  members: [
    {
      _id: 0,
      host: `${hostname}.${svcname}.${ns}.svc.cluster.local:27017`,
    },
  ],
};

try {
  rs.status();
} catch (e) {
  if (e.codeName === "NotYetInitialized" || e.code === 94) {
    rs.initiate(icfg);
    print("Replica set initialized");
  } else if (e.codeName === "Unauthorized" || e.code === 13) {
    print("Authentication required; assuming admin user exists and bootstrap is complete");
  } else {
    throw e;
  }
}
