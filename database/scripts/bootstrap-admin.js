const adminUsername = process.env.MONGO_ADMIN_USERNAME;
const adminPassword = process.env.MONGO_ADMIN_PASSWORD;

const adminDb = db.getSiblingDB("admin");

try {
  adminDb.createUser({
    user: adminUsername,
    pwd: adminPassword,
    roles: [{ role: "root", db: "admin" }],
  });
  print("Admin user created");
} catch (e) {
  print(`createUser failed: ${e}`);

  try {
    if (adminDb.auth(adminUsername, adminPassword)) {
      print("Admin user exists");
    } else {
      throw new Error("authentication failed");
    }
  } catch (authErr) {
    throw authErr;
  }
}
