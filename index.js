require("dotenv").config();

const database = process.env.DB_NAME;
const db = require("knex")({
  client: "mysql2",
  connection: {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database,

    // To use Hosted:
    ssl: {
      rejectUnauthorized: false,
    },
  },
  pool: { min: 0, max: 7 },
});

async function main() {
  await checkoutBranch("main");
  await getActiveBranch();

  // Start fresh so we can re-run this script
  await resetDatabase();

  await checkoutNewBranch("changes");

  // Build our tables
  await setupDatabase();
  await getTables();

  // Our first Dolt feature. This will commit the first time
  // But after that nothing has changed so there is nothing to commit.
  await doltCommit("Taylor <taylor@dolthub.com>", "Created tables");

  // Examine a Dolt system table: dolt_log
  await getCommitLog();

  // Load rows into the tables
  await insertData();
  await getSummary();

  // Show off dolt_status and dolt_diff
  await getStatus();
  await getDiff("employees");

  // Dolt commit our changes
  await doltCommit("Tim <tim@dolthub.com>", "Inserted data into tables");
  await getCommitLog();

  // Show off dolt_reset
  await dropTable("employee_teams");
  await getStatus();
  await getTables();
  await doltResetHard();
  await getStatus();
  await getTables();

  // Show off branch and merge
  await checkoutNewBranch("modify_data");
  await modifyData();
  await getStatus();
  await getDiff("employees");
  await getDiff("employee_teams");
  await getSummary();
  await doltCommit("Brian <brian@dolthub.com>", "Modified data on branch");
  await getCommitLog();

  // Switch back to changes because I want the same merge base
  await checkoutBranch("changes");
  await checkoutNewBranch("modify_schema");
  await getActiveBranch();
  await modifySchema();
  await getStatus();
  await getDiff("employees");
  await getSummary();
  await doltCommit("Taylor <taylor@dolthub.com>", "Modified schema on branch");
  await getCommitLog();

  // Show off merge
  await checkoutBranch("changes");
  await getActiveBranch();
  await getCommitLog();
  await getSummary();
  await doltMerge("modify_data");
  await getSummary();
  await getCommitLog();
  await doltMerge("modify_schema");
  await getSummary();
  await getCommitLog();

  await db.destroy();
}

main();

async function checkoutNewBranch(branch) {
  await db.raw(`CALL DOLT_CHECKOUT('-b', ?)`, [branch]);
  console.log("Using new branch:", branch);
}

async function checkoutBranch(branch) {
  await db.raw(`CALL DOLT_CHECKOUT(?)`, [branch]);
  console.log("Using branch:", branch);
}

async function getActiveBranch() {
  const branch = await db.raw(`SELECT ACTIVE_BRANCH()`);
  console.log("Active branch:", branch[0][0]["ACTIVE_BRANCH()"]);
}

async function resetDatabase() {
  await db.raw("CALL DOLT_BRANCH('-D', 'changes')");
  await db.raw("CALL DOLT_BRANCH('-D', 'modify_data')");
  await db.raw("CALL DOLT_BRANCH('-D', 'modify_schema')");
}

async function setupDatabase() {
  await db.schema.createTable("employees", (table) => {
    table.integer("id").primary();
    table.string("last_name");
    table.string("first_name");
  });
  await db.schema.createTable("teams", (table) => {
    table.integer("id").primary();
    table.string("name");
  });
  await db.schema.createTable("employee_teams", (table) => {
    table
      .integer("employee_id")
      .references("id")
      .inTable("employees")
      .primary();
    table.integer("team_id").references("id").inTable("teams").primary();
  });
}

async function getTables() {
  const res = await db.raw("SHOW TABLES");
  const tables = res[0]
    .map((table) => table[`Tables_in_${database}`])
    .join(", ");
  console.log("Tables in database:", tables);
}

async function doltCommit(author, msg) {
  const res = await db.raw(`CALL DOLT_COMMIT('--author', ?, '-Am', ?)`, [
    author,
    msg,
  ]);
  console.log("Created commit:", res[0][0].hash);
}

async function getCommitLog() {
  const res = await db
    .select("commit_hash", "committer", "message")
    .from("dolt_log")
    .orderBy("date", "desc");
  console.log("Commit log:");
  res.forEach((log) =>
    console.log(`${log.commit_hash}: ${log.message} by ${log.committer}`)
  );
}

async function insertData() {
  await db("employees")
    .insert([
      { id: 0, last_name: "Sehn", first_name: "Tim" },
      { id: 1, last_name: "Hendriks", first_name: "Brian" },
      { id: 2, last_name: "Son", first_name: "Aaron" },
      { id: 3, last_name: "Fitzgerald", first_name: "Brian" },
    ])
    .onConflict()
    .merge();

  await db("teams")
    .insert([
      { id: 0, name: "Engineering" },
      { id: 1, name: "Sales" },
    ])
    .onConflict()
    .merge();

  await db("employee_teams")
    .insert([
      { employee_id: 0, team_id: 0 },
      { employee_id: 1, team_id: 0 },
      { employee_id: 2, team_id: 0 },
      { employee_id: 0, team_id: 1 },
      { employee_id: 3, team_id: 1 },
    ])
    .onConflict()
    .merge();
}

async function getSummary() {
  // Get all employees columns because we change the schema
  const employeeCols = await db("employees").columnInfo();
  const cols = Object.keys(employeeCols)
    .filter((col) => col !== "id")
    .map((col) => `employees.${col}`);

  // Dolt supports up to 12 table joins. Here we do a 3 table join.
  const res = await db
    .select("teams.name", ...cols)
    .from("employees")
    .join("employee_teams", "employees.id", "employee_teams.employee_id")
    .join("teams", "teams.id", "employee_teams.team_id")
    .orderBy("teams.name", "asc");

  res.forEach((row) => {
    let startDate = "";
    if ("start_date" in row) {
      startDate = row.start_date;
    }

    console.log(`${row.name}: ${row.first_name} ${row.last_name} ${startDate}`);
  });
}

async function getStatus() {
  const res = await db.select("*").from("dolt_status");
  console.log("Status:");
  if (res.length === 0) {
    console.log("No tables modified");
  } else {
    res.forEach((row) => {
      console.log(`${row.table_name}: ${row.status}`);
    });
  }
}

async function getDiff(table) {
  const res = await db
    .select("*")
    .from(`dolt_diff_${table}`)
    .where("to_commit", "WORKING");
  console.table(res);
}

async function dropTable(table) {
  await db.schema.dropTable(table);
}

async function doltResetHard(commit) {
  if (commit) {
    await db.raw(`CALL DOLT_RESET('--hard', ?)`, [commit]);
    console.log("Resetting to commit:");
  } else {
    await db.raw(`CALL DOLT_RESET('--hard')`);
    console.log("Resetting to HEAD");
  }
}

async function modifyData() {
  try {
    await db.transaction(async (trx) => {
      await trx("employees")
        .where("first_name", "Tim")
        .update("first_name", "Timothy");

      await trx("employees").insert({
        id: 4,
        last_name: "Bantle",
        first_name: "Taylor",
      });

      await trx("employee_teams").insert({
        employee_id: 4,
        team_id: 0,
      });

      await trx("employee_teams")
        .where("employee_id", 0)
        .where("employee_id", 1)
        .del();
    });
  } catch (err) {
    console.error(err);
  }
}

async function modifySchema() {
  try {
    await db.transaction(async (trx) => {
      await trx.schema.alterTable("employees", (table) => {
        table.date("start_date");
      });

      await trx("employees").where("id", 0).update("start_date", "2018-08-06");
      await trx("employees").where("id", 1).update("start_date", "2018-08-06");
      await trx("employees").where("id", 2).update("start_date", "2018-08-06");
      await trx("employees").where("id", 3).update("start_date", "2021-04-19");
    });
  } catch (err) {
    console.error(err);
  }
}

async function doltMerge(branch) {
  const res = await db.raw(`CALL DOLT_MERGE(?)`, [branch]);
  console.log(res);
  console.log("Merge complete for ", branch);
  console.log(
    "Commit:",
    res[0][0].hash,
    "Fast forward:",
    res[0][0].fast_forward,
    "Conflicts:",
    res[0][0].conflicts
  );
}
