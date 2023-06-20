/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import {Command, Option} from 'commander';
import {runCommand} from './commands/run';
import {loadConfig} from './config';
import {configShowCommand} from './commands/config';
import {compileCommand} from './commands/compile';
import {
  createBigQueryConnectionCommand,
  createPostgresConnectionCommand,
  listConnectionsCommand,
  removeConnectionCommand,
  showConnectionCommand,
  testConnectionCommand,
} from './commands/connections';
import {createBasicLogger, silenceLoggers} from './log';
import {loadConnections} from './connections/connection_manager';

export function createCLI(): Command {
  const cli = new Command();

  if (process.env.NODE_ENV === 'test') {
    // silenct output and don't process.exit(1) when testing

    // NOTE! exitOverride is copied to commands upon command creation, so setting
    // this after a command is created will mean that command does NOT have the exit
    // overridden. Feels like a bug with Commander but whatever we can solve by
    // setting here before any subcommands are created.
    cli.exitOverride();
    cli.configureOutput({
      outputError: () => {},
    });
  } else {
    // Red "Error:" if exiting with error
    // todo some parse errors also output "error:", strip that
    cli.configureOutput({
      outputError: (str, write) => write(`\x1b[31mError:\x1b[0m ${str}`),
    });
    cli.showHelpAfterError('(add --help for additional information)');
  }

  // config, logging, connections
  cli.hook('preAction', (_thisCommand, _actionCommand) => {
    // if packaged, respect debug flag, but if not, debug = true
    let debug;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    if ((<any>process).pkg) {
      debug = cli.opts().debug;
    } else {
      if (process.env.NODE_ENV !== 'test') {
        // eslint-disable-next-line no-console
        console.log(
          'Running Malloy CLI unpackaged, defaulting to "debug" level output'
        );
      }
      debug = true;
    }

    if (debug) createBasicLogger('debug');
    else createBasicLogger(cli.opts()['log-level']);

    if (cli.opts().quiet) silenceLoggers();

    loadConfig(cli.opts().config);
    loadConnections();
  });

  // global options
  cli
    .version('0.0.1')
    .name('malloy')
    .addOption(
      new Option('-c, --config <file_path>', 'path to a config.json file').env(
        'MALLOY_CONFIG_FILE'
      )
    )
    .addOption(new Option('-q, --quiet', 'silence output'))
    .addOption(new Option('-d, --debug', 'print debug-level logs to stdout'))
    .addOption(
      new Option('-l, --log-level', 'log level')
        .choices(['error', 'warn', 'info', 'debug'])
        .default('warn')
    );

  // commands
  // TODO optional statement index
  // TODO dry run
  // TODO cost query?
  // TODO output format
  // TODO truncation
  // TODO named malloy query
  const runDescription = `execute a Malloy file (.malloy or .malloysql)

When executing a MalloySQL file, all statements in the file get run. If --index is passed,
the statement at that 1-based index is executed. If this statement is a SQL statement,
all Malloy statments above that statement are also executed (in case the SQL statement
has embedded Malloy).

When executing a Malloy file, only the final runnable query in file is executed. If one does
not exist, nothing will be executed, unles --index is passed. If --index is passd, the query
at that 1-based index will be executed. If --query-name is passed, the named query will be
executed`;
  cli
    .command('run <file>')
    .summary('execute a Malloy file (.malloy or .malloysql)')
    .description(runDescription)
    .addOption(
      new Option(
        '-i, --index <number>',
        'only run statement or query at index i (1-based)'
      ).conflicts('query-name')
    )
    .addOption(
      new Option(
        '-n, --query-name <name>',
        'run a named query (.malloy file only)'
      ).conflicts('index')
    )
    .action(runCommand);

  // TODO optional statement index
  // TODO output - how?
  cli
    .command('compile <file>')
    .description(
      'compile a Malloy file (.malloy or .malloysql) and output resulting SQL'
    )
    .action(compileCommand);

  const connections = cli
    .command('connections')
    .description('manage connection configuration');

  connections
    .command('list')
    .description('list all database connections')
    .action(listConnectionsCommand);

  connections
    .command('create-bigquery')
    .description('add a new BigQuery database connection')
    .argument('<name>')
    .addOption(new Option('-p, --project <id>'))
    .addOption(new Option('-l, --location <region>').default('US'))
    .option('-k, --service-account-key-path <path>')
    .addOption(new Option('-t, --timeout <milliseconds>').argParser(parseInt))
    .addOption(
      new Option('-m, --maximum-bytes-billed <bytes>').argParser(parseInt)
    )
    .action(createBigQueryConnectionCommand);

  connections
    .command('create-postgres')
    .description('add a new Postgres database connection')
    .argument('<name>')
    .option('-h, --host <url>')
    .option('-u, --username <name>')
    .addOption(new Option('-p, --port <number>').argParser(parseInt))
    .option('-d, --database-name <name>')
    .option('-p, --password <password>')
    .action(createPostgresConnectionCommand);

  connections
    .command('create-duckdb')
    .description('add a new DuckDB database connection')
    .argument('<name>');
  // TODO path to duckdb executable, otherwise assume ./duckdb

  connections
    .command('test')
    .description('test a database connection')
    .argument('<name>')
    .action(testConnectionCommand);

  connections
    .command('show')
    .description('show details for a database connection')
    .argument('<name>')
    .action(showConnectionCommand);

  // TODO should this be connection-specific?
  // connections.command('update').description('update a database connection');

  connections
    .command('delete')
    .description('remove a database connection')
    .argument('<name>')
    .action(removeConnectionCommand);

  cli
    .command('config')
    .description('output the current config')
    .action(configShowCommand);

  return cli;
}

export const cli = createCLI();
export async function run() {
  await cli.parseAsync(process.argv);
}
