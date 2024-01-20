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

import fs from 'fs';
import {
  ModelMaterializer,
  QueryMaterializer,
  Runtime,
} from '@malloydata/malloy';
import url, {fileURLToPath as fileURLToPath} from 'node:url';
import {
  CLIConnectionFactory,
  connectionManager,
} from '../connections/connection_manager';
import {
  QueryOptionsType,
  RunOrCompileOptions,
  StandardOutputType,
  getFilteredResultsLogger,
} from './util';

export async function runMalloy(
  filePath: string,
  options: RunOrCompileOptions
) {
  const resultsLog = getFilteredResultsLogger(
    options.json
      ? 'json'
      : [
          StandardOutputType.Malloy,
          StandardOutputType.CompiledSQL,
          StandardOutputType.Results,
          StandardOutputType.Tasks,
        ]
  );
  const json = {};

  let modelMaterializer: ModelMaterializer;
  const fileURL = url.pathToFileURL(filePath);

  const malloyRuntime = new Runtime(
    {
      readURL: async (url: URL) => {
        return fs.readFileSync(fileURLToPath(url), 'utf8');
      },
    },
    connectionManager.getConnectionLookup(fileURL)
  );

  try {
    if (!modelMaterializer) {
      modelMaterializer = malloyRuntime.loadModel(fileURL);
    } else {
      modelMaterializer.extendModel(fileURL);
    }

    let query: QueryMaterializer;
    if (options.queryOptions) {
      switch (options.queryOptions.type) {
        case QueryOptionsType.Index:
          query = await modelMaterializer.loadQueryByIndex(
            options.queryOptions.index
          );
          break;
        case QueryOptionsType.Name:
          query = await modelMaterializer.loadQueryByName(
            options.queryOptions.name
          );
          break;
        case QueryOptionsType.String:
          query = await modelMaterializer.loadQuery(
            `run: ${options.queryOptions.query}`
          );
          break;
      }
    } else query = await modelMaterializer.loadFinalQuery();

    // !!HACK!! (should eventually be fixed in malloy grammar)
    // this eliminates the errors related to intervals in snowflake <-> malloy
    //
    // As an example consider the sql generated for Snowflake to run a query like this
    // SELECT customer_id FROM purchases WHERE order_date >= CURRENT_DATE - INTERVAL '(1) DAY'
    // is invalid in snowflake dialect and we would rather need to change it to
    // SELECT customer_id FROM purchases WHERE order_date >= CURRENT_DATE - INTERVAL '1 DAY'
    let sql = await query.getSQL();
    sql = sql.replace(/'\((\d+)\) ([a-zA-Z]+)'/g, "'$1 $2'");
    json['sql'] = sql.trim();

    if (options.compileOnly) {
      resultsLog.sql('Compiled SQL:');
      resultsLog.sql(sql);

      return JSON.stringify(json);
    }
    const configs = await connectionManager.getAllConnectionConfigs();

    let bokksu_config;
    for (const config of configs) {
      if (config.name === 'bokksu') {
        bokksu_config = config;
      }
    }
    if (bokksu_config) {
      const factory = new CLIConnectionFactory();
      const conn = await factory.getConnectionForConfig(bokksu_config);
      const results = await conn.runSQL(sql);
      resultsLog.result(JSON.stringify(results, null, 2));
      return JSON.stringify(results);
    }

    const results = await query.run();
    resultsLog.result(
      JSON.stringify(results.toJSON().queryResult.result, null, 2)
    );
    json['results'] = JSON.stringify(results.toJSON().queryResult.result);

    return JSON.stringify(json);
  } catch (e) {
    resultsLog.error(e);
  }
}
