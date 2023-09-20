require("dotenv").config();
const knex = require("knex");
const fs = require("fs");

const database = process.env.DB_NAME;

const db = knex({
  client: "mysql2",
  connection: {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,

    ssl: process.env.DB_SSL_PATH
      ? { ca: fs.readFileSync(__dirname + process.env.DB_SSL_PATH) } // Can download certificate from Hosted Dolt
      : false,
  },
  pool: { min: 0, max: 7 },
});

async function main() {
  await checkoutBranch("main");
  await printActiveBranch();

  // Start fresh so we can re-run this script
  await resetDatabase();

  // Build our tables
  await setupDatabase();
  await printTables();

  // Our first Dolt feature. This will commit the first time
  // But after that nothing has changed so there is nothing to commit.
  await doltCommit("Taylor <taylor@dolthub.com>", "Created tables");

  // Examine a Dolt system table: dolt_log
  await printCommitLog();

  // Load rows into the tables
  await insertData();
  await printSummaryTable();

  // Show off dolt_status and dolt_diff
  await printStatus();
  await printDiff("employees");

  // Dolt commit our changes
  await doltCommit("Tim <tim@dolthub.com>", "Inserted data into tables");
  await printCommitLog();

  // Show off dolt_reset
  await dropTable("employees_teams");
  await printStatus();
  await printTables();
  await doltResetHard();
  await printStatus();
  await printTables();

  // // // Show off branch and merge
  await createBranch("modify_data");
  await checkoutBranch("modify_data");
  await printActiveBranch();
  await modifyData();
  await printStatus();
  await printDiff("employees");
  await printDiff("employees_teams");
  await printSummaryTable();
  await doltCommit("Brian <brian@dolthub.com>", "Modified data on branch");
  await printCommitLog();

  // // Switch back to main because I want the same merge base
  await checkoutBranch("main");
  await createBranch("modify_schema");
  await checkoutBranch("modify_schema");
  await printActiveBranch();
  await modifySchema();
  await printStatus();
  await printDiff("employees");
  await printSummaryTable();
  await doltCommit("Taylor <taylor@dolthub.com>", "Modified schema on branch");
  await printCommitLog();

  // // Show off merge
  await checkoutBranch("main");
  await printActiveBranch();
  await printCommitLog();
  await printSummaryTable();
  await doltMerge("modify_data");
  await printSummaryTable();
  await printCommitLog();
  await doltMerge("modify_schema");
  await printSummaryTable();
  await printCommitLog();

  await db.destroy();
}

main();

async function createBranch(branch) {
  const res = await db
    .select("name")
    .from("dolt_branches")
    .where("name", branch);
  if (res.length > 0) {
    console.log("Branch exists:", branch);
  } else {
    await db.raw(`CALL DOLT_BRANCH(?)`, [branch]);
    console.log("Created branch:", branch);
  }
}

async function checkoutBranch(branch) {
  await db.raw(`CALL DOLT_CHECKOUT(?)`, [branch]);
  console.log("Using branch:", branch);
}

async function printActiveBranch() {
  const branch = await db.raw(`SELECT ACTIVE_BRANCH()`);
  console.log("Active branch:", branch[0][0]["ACTIVE_BRANCH()"]);
}

async function resetDatabase() {
  const logs = await db
    .select("commit_hash")
    .from("dolt_log")
    .limit(1)
    .orderBy("date", "asc");
  await doltResetHard(logs[0].commit_hash);

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
  await db.schema.createTable("employees_teams", (table) => {
    table
      .integer("employee_id")
      .references("id")
      .inTable("employees")
      .primary();
    table.integer("team_id").references("id").inTable("teams").primary();
  });
}

async function printTables() {
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

async function printCommitLog() {
  const res = await db
    .select("commit_hash", "committer", "message")
    .from("dolt_log")
    .orderBy("date", "desc");
  console.log("Commit log:");
  res.forEach((log) =>
    console.log(`  ${log.commit_hash}: ${log.message} by ${log.committer}`)
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

  await db("employees_teams")
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

async function printSummaryTable() {
  // Get all employees columns because we change the schema
  const colInfo = await db("employees").columnInfo();
  const employeeCols = Object.keys(colInfo)
    .filter((col) => col !== "id")
    .map((col) => `employees.${col}`);

  // Dolt supports up to 12 table joins. Here we do a 3 table join.
  const res = await db
    .select("teams.name", ...employeeCols)
    .from("employees")
    .join("employees_teams", "employees.id", "employees_teams.employee_id")
    .join("teams", "teams.id", "employees_teams.team_id")
    .orderBy("teams.name", "asc");

  console.log("Summary:");
  res.forEach((row) => {
    let startDate = "";
    if ("start_date" in row) {
      if (row.start_date === null) {
        startDate = "None";
      } else {
        const d = new Date(row.start_date);
        startDate = d.toDateString();
      }
    }
    console.log(
      `  ${row.name}: ${row.first_name} ${row.last_name} ${startDate}`
    );
  });
}

async function printStatus() {
  const res = await db.select("*").from("dolt_status");
  console.log("Status:");
  if (res.length === 0) {
    console.log("  No tables modified");
  } else {
    res.forEach((row) => {
      console.log(`  ${row.table_name}: ${row.status}`);
    });
  }
}

async function printDiff(table) {
  const res = await db
    .select("*")
    .from(`dolt_diff_${table}`)
    .where("to_commit", "WORKING");
  console.log(`Diff for ${table}:`);
  console.log(res);
}

async function dropTable(table) {
  await db.schema.dropTable(table);
}

async function doltResetHard(commit) {
  if (commit) {
    await db.raw(`CALL DOLT_RESET('--hard', ?)`, [commit]);
    console.log("Resetting to commit:", commit);
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

      await trx("employees_teams").insert({
        employee_id: 4,
        team_id: 0,
      });

      await trx("employees_teams")
        .where("employee_id", 0)
        .where("employee_id", 1)
        .del();
    });
  } catch (err) {
    // Rolls back transaction
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
    // Rolls back transaction
    console.error(err);
  }
}

async function doltMerge(branch) {
  const res = await db.raw(`CALL DOLT_MERGE(?)`, [branch]);
  console.log("Merge complete for ", branch);
  console.log(`  Commit: ${res[0][0].hash}`);
  console.log(`  Fast forward: ${res[0][0].fast_forward}`);
  console.log(`  Conflicts: ${res[0][0].conflicts}`);
}
