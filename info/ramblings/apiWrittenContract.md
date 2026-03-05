# The big sigma's rizz (written) contract 

## Core thingies

- **Dataset**: uploaded CSV + schema + basic stats
- **QuerySpec**: replayable dataset query (filters/sort/select/pagination)
- **SavedView**: named, persisted QuerySpec
- **ModelJob**: async/sync training job (status + logs + resulting model_id) # idk this is never seeing prod lol
- **ModelArtifact**: trained model metadata + metrics + coefficients summary
- **PredictionJob**: inference request (optionally async) + results

---

## Conventions

- All timestamps are ISO 8601 strings.
- IDs are ints(pythong BIGINTBIGINTBIGINTBIGINTBIGINTBIGINT).
- App/domain errors from `ApiException` use `{ error: { code, message, details? } }`.
- FastAPI request validation/parsing errors keep default `{ detail: ... }` shape.
- Offset pagination default is `limit=50` and `offset=0` (max `limit=1000`) for list/query endpoints.
- `POST .../query` returns `rows` `total_rows` `returned_rows` `applied_query` `next_offset`.
- CONCURRENT JOBS - MUTEXES? MAYBE!!! LOCKS?? MAYBE!!! I DIDN'T TAKE COMP 409, BECAUSE I HAD TO MAKE ROOM FOR 595 (bars)

---

## Common Types

```ts
type ISODateTime = string
type ID = number

type ApiError = {
  error: {
    code: string
    message: string
    details?: unknown
  }
}

type JobStatus = "queued" | "running" | "completed" | "failed" | "canceled"
```

## Data stuff
```ts
type Dataset = {
  id: ID
  name: string
  filename: string

  row_count: number
  column_count: number
  columns: ColumnInfo[]
    
  created_at: ISODateTime
}

type ColumnInfo = {
  name: string
  dtype: "int" | "float" | "string" | "bool" | "datetime" | "unknown"
  nullable: boolean
    
  unique_count?: number
  null_count?: number
}

type NumericSummary = {
  min: number
  max: number
  mean: number
  std: number
}

type ColumnStats = ColumnInfo & {
  summary?: NumericSummary // present for numeric cols
}

type DatasetStats = {
  dataset_id: ID
  row_count: number
  column_count: number
  columns: ColumnStats[]
}

```

## Query tings
```ts
type QuerySpec = {
  select?: string[]              // columns to return (default = all)
  y_columns?: string[]           // target columns for this view/query context
  filters?: FilterClause[][]     // OR of AND clauses: outer list is OR, each inner list is AND
  sort?: SortClause[]            // ORDER BY
  limit?: number                 // table pagination override (default 50, max 1000)
  offset?: number                // table pagination override (default 0)


  // UX helper maybe
  highlights?: HighlightRule[]
}

type QuerySpecPatch = {
  select?: string[]              // columns to return
  y_columns?: string[]           // target columns for this view/query context
  filters?: FilterClause[][]
  sort?: SortClause[]
  limit?: number                 // table pagination override, max 1000
  offset?: number                // table pagination override
  highlights?: HighlightRule[]
}

type FilterOp =
  | "eq" | "neq"
  | "lt" | "lte" | "gt" | "gte"
  | "contains" | "starts_with" | "ends_with"
  | "in" | "not_in"
  | "is_null" | "not_null"
  | "between"

type FilterValue = string | number | boolean | null
type FilterClause = {
  column: string
  op: FilterOp
  value?: FilterValue | FilterValue[]
}

// semantics:
// filters = [
//   [a, b],   // a AND b
//   [c]       // OR c
// ]

type SortClause = {
  column: string
  direction: "asc" | "desc"
}

type HighlightRule = {
  column: string
  op: "eq" | "lt" | "gt" | "between" | "contains"
  value: string | number | boolean | (string | number)[]
  label?: string
}

type SavedView = {
  id: ID
  dataset_id: ID
  name: string
  query: QuerySpec               // persisted reusable view logic (+ optional y_columns)
  created_at: ISODateTime
}

type QueryResponse = {
  rows: Record<string, unknown>[]
  total_rows: number             // after filters, before pagination
  returned_rows: number          // size of rows in this response
  next_offset: number | null     // null => no more rows
  applied_query: QuerySpec
}
```

## Modeling tings
```ts
type ModelType =
    | "linear_regression"
    | "ridge_regression"
    | "lasso_regression"
    | "logistic_regression"
    | "softmax_regression"

type RegularizationType =
    | RegressionRegularization | "none"

type RegressionRegularization = 
    | "ridge" | "lasso"

type TrainType =
  | {
      kind: "closed_form"
      // really only for linear regression and l2 ig
      // not for logistic / softmax, and not for true L1
      solver?: "normal_equation" | "qr" | "svd"
    }
  | FirstOrderGradientDescent

type FirstOrderGradientDescent = {
  kind: "first_order_gd"

  // how gradient is estimated
  batch_mode: "full_batch" | "sgd" | "mini_batch"
  batch_size?: number // required for mini_batch, else defaults to 1 / -1

  // gradient update
  optimizer:
    | "plain" // - delta j * a
    | "momentum" // beta 
    | "nesterov" // todo figure out what this is
    | "adagrad" // ts converges eraly sometimes
    | "rmsprop" // ts is adagrad if it didn't converge eraly soetimes
    | "adam" // hi adam
    
}
```

## specs
```ts
type SplitSpec = {
  validation_holdout?: number     // e.g. 0.2
  test_holdout?: number           // e.g. 0.1
  random_seed?: number            // default fixed (e.g. 67)
  shuffle?: boolean               // default true (ignored if time series)
  is_time_series?: boolean        // if true, split chronologically
}

type PreprocessSpec = {
  drop_null_rows?: boolean

  // numeric
  standardize?: boolean           // z-score
  normalize?: boolean             // min-max

  // categorical
  one_hot_encode?: boolean

  // null handling 
  fill_nulls?: "mean" | "median" | "mode" | "zero"
}

```


## hyperparam thingamabobs
```ts
type HyperParamSpec = {
    learning_rate?: number | null
    epochs?: number | null
    batch_size?: number | null

    // regularization
    lambda_reg?: number | null
}

type TuneSpec = {
    enabled: boolean
    // tune null hyperparams (idk prolly do default vals if false)
    max_trials?: number            // e.g. 20
    search?: "grid" | "random"
}
```

## training the gerberts
```ts
type TrainModelRequest = {
  dataset_id: ID
  name?: string

  model_type: ModelType
  train_type: TrainType

  x_cols: string[]
  y_cols: string[]               // multi-target, might freak up 2-d visuals 

  // optional subset before training
  query?: QuerySpecPatch

  split?: SplitSpec
  preprocessing?: PreprocessSpec

  hyperparams: HyperParamSpec
  tuning?: TuneSpec
}

type ModelJob = {
  id: ID
  status: JobStatus
  request: TrainModelRequest

  created_at: ISODateTime
  started_at?: ISODateTime
  finished_at?: ISODateTime

  // logging
  progress?: number              // 0..1
  logs?: string[]                // last N messages

  error?: string
  model_id?: ID                  // present when completed
}
```

## training request constraints (v1)

Validation failure returns `422`.

* `x_cols` must be non-empty, unique, and exist in dataset columns.
* `y_cols` must be non-empty, unique, and exist in dataset columns.
* `x_cols` and `y_cols` must be disjoint.
* `model_type="linear_regression" | "ridge_regression" | "lasso_regression"` requires exactly 1 `y_col`, and it must be numeric.
* `model_type="logistic_regression"` requires exactly 1 `y_col`, and it must be binary.
* `model_type="softmax_regression"` requires exactly 1 `y_col`, and it must have 3+ classes.
* `train_type.kind="closed_form"` is only allowed for `linear_regression` and `ridge_regression`.
* `train_type.kind="first_order_gd"` is required for `lasso_regression`, `logistic_regression`, and `softmax_regression`.
* `train_type.batch_mode="mini_batch"` requires `batch_size >= 2`.
* `train_type.batch_mode="sgd"` means effective `batch_size=1`; if `batch_size` is sent, it must be `1`.
* `train_type.batch_mode="full_batch"` must omit `batch_size`.
* For `first_order_gd`, `hyperparams.learning_rate` and `hyperparams.epochs` are required and must be `> 0`.
* For `closed_form`, `hyperparams.learning_rate`, `hyperparams.epochs`, and `hyperparams.batch_size` must be null/omitted.
* `lambda_reg` is required and `> 0` for `ridge_regression` and `lasso_regression`; null/omitted for all others.
* If present, `split.validation_holdout` and `split.test_holdout` must each be in `[0,1)` and their sum must be `< 1`.
* `preprocessing.standardize` and `preprocessing.normalize` cannot both be `true`.

## accessing and testing the gerberts(2 2s my word fam they grow up so fast )
```ts
type MetricSet = {
  // classification
  accuracy?: number
  precision?: number
  recall?: number
  f1?: number
  log_loss?: number

  // regression
  mse?: number
  rmse?: number
  mae?: number
  r2?: number
}

type ModelMetrics = {
  train?: MetricSet
  validation?: MetricSet
  test?: MetricSet
}

type ModelArtifact = {
  id: ID
  name: string

  dataset_id: ID
  model_type: ModelType
  train_type: TrainType

  x_cols: string[]
  y_cols: string[]

  split?: SplitSpec
  preprocessing?: PreprocessSpec

  hyperparams: HyperParamSpec
  tuning?: {
    enabled: boolean
    searched_fields?: string[]
    best_hyperparams?: HyperParamSpec
  }

  metrics: ModelMetrics

  // expose for UI
  coefficients?: {
    feature: string
    value: number
  }[]

  created_at: ISODateTime
}
```

## predicting the future 
```ts
type PredictRequest = {
    model_ids: ID[]
    dataset_id: ID
    query?: QuerySpecPatch          // subset to predict on
}
type PredictionRow = {
    row_index: number
    prediction: Record<string, unknown>
    class_scores?: Record<string, number> // classification probs by class label
    confidence?: number // optional scalar confidence, usually 0..1
    y_true?: Record<string, unknown> // present only if ground truth exists for the row
}
type PredictionResult = {
    model_id: ID
    predictions: PredictionRow[]
    metrics?: MetricSet             // if ground truth available
}
type PredictionJob = {
    id: ID
    status: JobStatus
    request: PredictRequest

    created_at: ISODateTime
    started_at?: ISODateTime
    finished_at?: ISODateTime

    error?: string
    results?: PredictionResult[]    // present when completed
}
```

## prediction request constraints (v1)

Validation failure returns `422`.

* `model_ids` must be non-empty and unique.
* Every `model_id` must exist and be in completed state.
* For each model, all required `x_cols` must exist in target `dataset_id`.
* If `query.select` is provided, server auto-includes required `x_cols` so prediction can run.
* `metrics` and `y_true` are only included when ground-truth labels are available in the queried rows.

---
## Endpoints (v1)

Base URL: `/v1`

Errors:

App/domain error envelope (`ApiException`):

```json
{
  "error": { "code": "string", "message": "string", "details": {} }
}
```

FastAPI validation/parsing error envelope:

```json
{
  "detail": [
    {
      "loc": ["body", "..."],
      "msg": "string",
      "type": "string"
    }
  ]
}
```

---

# Health

### `GET /v1/health`

Returns `200` if service is up. i mean there is no service (unless i do a portfolio vercel hosting thing but i think you do mocks so it's all UI in those cases). but hey good for debug amirite

```json
{ "ok": true, "time": "2026-03-04T12:34:56Z" }
```

---

# Datasets

### `POST /v1/datasets`

Create a dataset by uploading a CSV.

* Content-Type: `multipart/form-data`
* Fields:

  * `file`: CSV file
  * `name` (optional): string

Response `201` → `Dataset`, that means it was made sucessufly 😻 # learningCodesAndLearningToCodeWithBeen

```json
{
  "id": 1,
  "name": "SpamBase",
  "filename": "spambase.csv",
  "row_count": 4601,
  "column_count": 58,
  "columns": [{ "name": "word_freq_free", "dtype": "float", "nullable": false }],
  "created_at": "2026-03-04T12:34:56Z"
}
```

### `GET /v1/datasets`

List datasets.

* Query params (optional): `limit`, `offset`, offset is for skipping, we all paginate down here(in florida(in the Us(in north america(in eart(insolarsystem(hi (this probably counts as cosmic horror(are you scared(yeah me neither)))))))))

Response `200`

```json
{
  "items": [ /* Dataset[] */ ],
  "total": 12,
  "limit": 50,
  "offset": 0
}
```

### `GET /v1/datasets/:dataset_id`

Get one dataset.

Response `200` → `Dataset`

### `DELETE /v1/datasets/:dataset_id`

Delete dataset (and dependent artifacts/jobs, or reject if present(i did that at my last work but for {I DID NOT SIGN AN NDA} ). idk which one to pick but that's not an api so take that

Response `204`, that's basically a void success

### `GET /v1/datasets/:dataset_id/stats`

Basic dataset/column stats (shape, nulls, uniques, and optional numeric summaries).
Response `200` → `DatasetStats`

```json
{
  "dataset_id": 1,
  "row_count": 6767,
  "column_count": 67,
  "columns": [
    {
      "name": "word_freq_free",
      "dtype": "float",
      "nullable": false,
      "null_count": 0,
      "unique_count": 312,
      "summary": { "min": 0, "max": 4.54, "mean": 0.11, "std": 0.32 }
    }
  ]
}
```

---

# Querying rows

### `POST /v1/datasets/:dataset_id/query`

Body: `QuerySpec`
Response `200` → `QueryResponse` (always)

```json
{
  "rows": [{ "word_freq_free": 0.21, "is_spam": 1 }],
  "total_rows": 4601, // thos who do logrg k fold CV on uci spam base 💀💀
  "returned_rows": 1,
  "next_offset": 1,
  "applied_query": { "select": ["word_freq_free", "is_spam"], "limit": 50, "offset": 0 }
}
```

Notes:

* `select` omitted ⇒ all columns
* `limit` omitted ⇒ `50`
* `offset` omitted ⇒ `0`
* `total_rows` = rows matching filters before limit/offset
* `next_offset` = null when no more rows are available

---

# SavedViews

### `POST /v1/datasets/:dataset_id/views`

Create a named saved query.
Body:

```json
{ "name": "High FREE", "query": { "filters": [{ "column": "word_freq_free", "op": "gt", "value": 0.5 }] } }
```

Response `201` → `SavedView`

### `GET /v1/datasets/:dataset_id/views`

List saved views for dataset.
* Query params (optional): `limit`, `offset`
Response `200`

```json
{ "items": [ /* SavedView[] */ ], "total": 4 }
```

### `GET /v1/views/:view_id`

Get one saved view.
Response `200` → `SavedView`

### `PUT /v1/views/:view_id`

Update `name` and/or `query`.
Response `200` → `SavedView`

### `DELETE /v1/views/:view_id`

Response `204`

### `POST /v1/views/:view_id/query`

Run a saved view (optionally override parts like `select/limit/sort`).
Body (optional): partial `QuerySpec` merged over stored query.

Response `200` → `QueryResponse`

Merge rules (this matters for the API behavior):

* If a field is omitted in override body, stored value is kept.
* If a field is present in override body, it replaces stored value for that field.
* Arrays (`select`, `filters`, `sort`, `highlights`) replace fully; no element-wise merge.
* `applied_query` in response is the final merged query the backend executed.

---

# ModelArtifacts (trained models)

### `GET /v1/models`

List models.

* Query params (optional): `dataset_id`, `limit`, `offset`
* `model_type` is accepted by the route currently but ignored (reserved for future filtering)

Response `200`

```json
{ "items": [ /* ModelArtifact[] */ ], "total": 10, "limit": 50, "offset": 0 }
```

### `GET /v1/models/:model_id`

Get a trained model artifact.
Response `200` → `ModelArtifact`

### `DELETE /v1/models/:model_id`

Delete a model artifact.
Response `204`

---

# ModelJobs (training)

### `POST /v1/model-jobs`

Start training.
Body: `TrainModelRequest`

Response `202` → `ModelJob`

```json
{
  "id": 67,
  "status": "queued",
  "request": { /* TrainModelRequest */ },
  "created_at": "2026-03-04T12:34:56Z",
  "progress": null
}
```

### `GET /v1/model-jobs/:job_id`

Poll status.
Response `200` → `ModelJob`

* When `status="completed"`, `model_id` is present.

### `GET /v1/model-jobs/:job_id/logs`

Fetch logs.

* Query params (optional): `tail` (default 200), `since_index`
  Response `200`

```json
{ "job_id": 67, "logs": ["epoch 1 loss=...", "epoch 2 loss=..."], "next_index": 128 }
```

### `POST /v1/model-jobs/:job_id/cancel`

Cancel a queued/running job.
Response `200` → `ModelJob`

---

# Prediction (sync + async)

v1 decision: keep both endpoints.

* `POST /v1/predict` is for smaller requests that should return immediately.
* `POST /v1/prediction-jobs` is for larger/longer runs.

## Sync prediction

### `POST /v1/predict`

Run prediction synchronously because.
Body: `PredictRequest`

Current backend behavior:

* Returns `501` with `code="PREDICTION_NOT_IMPLEMENTED"` until prediction service is implemented.

Planned behavior once implemented: response `200`

If request is too large for sync policy, return `409` with:

```json
{
  "error": {
    "code": "PREDICT_SYNC_LIMIT",
    "message": "Request exceeds synchronous prediction limits; use /v1/prediction-jobs",
    "details": { "max_rows": 10000, "max_models": 5 }
  }
}
```

```json
{
  "results": [
    {
      "model_id": 7,
      "predictions": [
        { "row_index": 123, "prediction": { "y": 1 }, "class_scores": { "spam": 0.91, "ham": 0.09 }, "y_true": { "y": 1 } }
      ],
      "metrics": { "accuracy": 0.93, "f1": 0.91, "log_loss": 0.24 }
    }
  ]
}
```

## Async prediction

### `POST /v1/prediction-jobs`

Create an async prediction job.
Body: `PredictRequest`

Response `202` → `PredictionJob`

### `GET /v1/prediction-jobs`
Poll all statuses
* Query params (optional): `dataset_id`, `status`, `limit`, `offset`
Response `200` → `list[PredictionJob]`
### `GET /v1/prediction-jobs/:job_id`

Poll status.
Response `200` → `PredictionJob`

### `GET /v1/prediction-jobs/:job_id/results`

Fetch results (when completed).
Response `200` → `PredictionResult[]`

### `POST /v1/prediction-jobs/:job_id/cancel`

Cancel a queued/running prediction job.
Response `200` → `PredictionJob`

---

# Beens status code guide
lol imagine learning from inline annotations

* `200` OK (reads, sync runs)
* `201` Created (dataset/view created)
* `202` Accepted (async jobs created)
* `204` No Content (deletes)
* `400` invalid request / schema mismatch
* `404` not found
* `409` conflict (e.g., delete blocked, concurrent job policy)
* `422` semantic validation failed (query/train/predict rules)
* `501` feature not implemented yet
* `500` internal error
