# My Kafka Store V5 — Path B: Databricks Free Edition + Azure Data Lake Storage
### Kafka → Parquet → **Databricks Volume + Azure Data Lake (ADLS Gen2)** → Auto Loader → Bronze / Silver / Gold

*Starts right after you finished **Path A in V4**. V4 stays exactly as it is, as your working backup.*

---

## What Path B Adds (and what it doesn't change)

Path B = **Path A + every Parquet file also saved in your own Azure Data Lake**.

```
 Kafka ─► lake_writer.py ─► data-lake/landing/ ─► lake_uploader.py
                                                     │
                              LAKE_SINKS=volume,adls │
                         ┌───────────────────────────┴─────────────────────┐
                         ▼                                                 ▼
          Databricks Free Edition Volume                   Azure Data Lake Storage Gen2
   /Volumes/workspace/kafka_store_v5/landing/raw    abfss://landing@<account>.dfs.core.windows.net/raw
                         │                                                 │
           file arrival trigger                                 (your own copy — used by
                         ▼                                       Path C / v6 later)
      Job: bronze → silver → gold  (schema kafka_store_v5)
```

| Thing | Changes in V5? |
|---|---|
| Python code (`app.py`, services, `sinks.py`, `lake_uploader.py`, `lake_writer.py`) | **No.** `sinks.py` already has the Azure uploader (`AdlsSink`). |
| `lake_config.py` | One line (a default value) |
| `.env` | A few values |
| Databricks notebooks (`00_setup`, `01`, `02`, `03`) | **No.** They take `catalog` / `schema` as parameters. |
| Databricks | New schema `kafka_store_v5` + a cloned job pointing at it |
| Azure | **New:** account, budget, resource group, storage account, container, SAS token |

> **Why a new schema `kafka_store_v5`?** V4 and V5 share one Free Edition workspace. If V5 wrote to `kafka_store` it would mix into (and change) V4's tables. A separate schema keeps V4's results untouched.

---

## Checklist (tick as you go)

- [ ] Part 1 — Wrap up V4
- [ ] Part 2 — Create the V5 project *(you've already done most of this)*
- [ ] Part 3 — Start Docker for V5 (fresh database)
- [ ] Part 4 — Databricks: create the `kafka_store_v5` schema and volumes
- [ ] Part 5 — Azure: create your Data Lake Storage
- [ ] Part 6 — Configure V5 (`.env`, `lake_config.py`) and test the connection
- [ ] Part 7 — Databricks: clone the job for V5
- [ ] Part 8 — End-to-end test
- [ ] Part 9 — Save V5 to GitHub
- [ ] Part 10 — Shutdown and cost safety

---

---

# PART 1 — Wrap Up V4

---

Do this in PyCharm's terminal inside `C:\Users\admin\PycharmProjects\test-v4\my-kafka-store-v4`.

**1. Make sure everything is committed and tagged** *(✅ already done — `v4.0` exists)*

```powershell
git status
git tag
```

`git status` should say *nothing to commit, working tree clean*, and `git tag` should list `v4.0`.

**2. Stop V4's containers** (V4 and V5 use the same container names and ports, so only one can run at a time):

```powershell
docker compose down
```

> ⚠️ Never add `-v` to `docker compose down`. `-v` deletes the database volume.

**3. Pause V4's Databricks job trigger** so it doesn't use your daily compute quota:

Databricks → **Jobs & Pipelines** → `kafka-store-medallion` → right panel **Schedules & Triggers** → set the file arrival trigger to **Paused** → **Save**.

---

---

# PART 2 — Create the V5 Project

---

*You've already done this part. Run the check commands to confirm, then move on.*

| Step | What | Status |
|---|---|---|
| 2.1 | Create empty GitHub repo `my-kafka-store-v5` (no README) | ✅ done |
| 2.2 | Clone V4 into `C:\Users\admin\PycharmProjects\test-v5\my-kafka-store-v5` | ✅ done |
| 2.3 | Rename `origin` → `v4`, add `origin` = V5 repo, push | ✅ done |
| 2.4 | Open in PyCharm (New Window), create `.venv`, `pip install -r requirements.txt` | ✅ done |
| 2.5 | Copy `.env` from V4 | ✅ done |

**Check** (inside `test-v5\my-kafka-store-v5`, with `.venv` activated):

```powershell
git remote -v
pip show databricks-sdk azure-storage-file-datalake
```

- `origin` must be `https://github.com/bertcuyugan/my-kafka-store-v5.git` and `v4` the V4 repo.
- Both packages should be listed. If `azure-storage-file-datalake` is missing: `pip install -r requirements.txt`.

<details>
<summary>For reference — the commands used to create V5 (use the same pattern for V6)</summary>

```powershell
mkdir C:\Users\admin\PycharmProjects\test-v5
cd C:\Users\admin\PycharmProjects\test-v5
git clone https://github.com/bertcuyugan/my-kafka-store-v4.git my-kafka-store-v5
cd my-kafka-store-v5
git remote rename origin v4
git remote add origin https://github.com/bertcuyugan/my-kafka-store-v5.git
git push -u origin main
git push origin --tags
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy ..\..\test-v4\my-kafka-store-v4\.env .env
```
</details>

> **Why wasn't the V4 guide copied into V5?** `.gitignore` contains `*.md` / `!README.md`, so Git ignores every Markdown file except the README. That's handy for keeping guides private. Save **this** guide in the V5 folder too; Git will ignore it the same way.

---

---

# PART 3 — Start Docker for V5 (Fresh Database)

---

Docker Compose names the database volume after the folder, so V5 gets its **own, empty** PostgreSQL database (`my-kafka-store-v5_pgdata`). V4's data stays safe in its own volume.

**1. Open Docker Desktop** and wait for it to be running.

**2. Start Kafka and PostgreSQL:**

```powershell
cd C:\Users\admin\PycharmProjects\test-v5\my-kafka-store-v5
docker compose up -d
docker ps
```

You should see `broker` and `postgres` with status `Up`.

**3. Create the Kafka topics:**

```powershell
docker exec --workdir /opt/kafka/bin/ -it broker bash
```

Inside the container:

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-placed --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payment-received --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic item-shipped --partitions 3 --replication-factor 1
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic item-delivered --partitions 3 --replication-factor 1
exit
```

(If one says *already exists*, that's fine.)

**4. Seed the products** (the new database is empty):

```powershell
python seed_products.py
```

> Your V4 login won't exist in V5's database. Register a new user in the store during the test (Part 8).

---

---

# PART 4 — Databricks: Create the `kafka_store_v5` Schema and Volumes

---

Same as V4's Part 2, Step 2, with a different schema name.

1. Databricks → **SQL Editor** (left sidebar).
2. Make sure **Serverless Starter Warehouse** is selected (top right). If a pop-up asks you to attach compute, choose **SQL Warehouse → Serverless Starter Warehouse → Attach and run**.
3. Run:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.kafka_store_v5;

CREATE VOLUME IF NOT EXISTS workspace.kafka_store_v5.landing
  COMMENT 'Raw Kafka event files uploaded by lake_uploader.py (V5)';

CREATE VOLUME IF NOT EXISTS workspace.kafka_store_v5.pipeline_state
  COMMENT 'Auto Loader checkpoints (V5)';

SHOW VOLUMES IN workspace.kafka_store_v5;
```

You should see two volumes: `landing` and `pipeline_state`.

> **Your Personal Access Token (PAT)** from V4 works for V5 too. It's already in `.env`. You only need a new one if it has expired (`401` / `PermissionDenied` later).

---

---

# PART 5 — Azure: Create Your Data Lake Storage

---

*This is the main new learning in Path B. Take it slowly; the portal has a lot of screens.*

## 5.0 — The picture

```
Azure account (your login)
 └── Subscription               ← the billing "wallet"
      └── Resource group        ← rg-kafka-store   (delete it = delete everything inside)
           └── Storage account  ← e.g. stkafkastoresubs01  (ADLS Gen2)
                └── Container   ← landing
                     └── raw/order-placed/2026-10-01/batch_....parquet
```

| Term | Meaning |
|---|---|
| **ADLS Gen2** | Blob Storage with **hierarchical namespace** turned on, so it has real folders. Databricks expects this. |
| **Region** | Use **Australia East** for everything. |
| **LRS** | Cheapest redundancy (3 copies in one data centre). Fine for learning. |
| **SAS token** | A time-limited "key card" with only the permissions you choose. The uploader uses this. |

**Cost:** this project stores a few megabytes, so expect cents per month after any free credit runs out. You'll set a budget alert first anyway.

## 5.1 — Create an Azure account

1. Go to https://azure.microsoft.com/free → **Try Azure for free** / **Start free**.
2. Sign in with a Microsoft account (you can create one with your Gmail) or GitHub.
3. Verify your phone number.
4. Enter a card for identity verification. You're **not charged** unless you upgrade to pay-as-you-go.
5. Open the portal: **https://portal.azure.com**

> 💡 Use the **search bar at the top** of the portal to find anything (Storage accounts, Resource groups, Budgets…).
> The free-credit offer changes over time; check the sign-up page for the current one.

## 5.2 — Set a budget alert (do this first!)

1. Search **Cost Management** → **Budgets** → **+ Add**.
2. **Scope:** your subscription.
3. **Name:** `kafka-store-budget` · **Reset period:** Monthly · **Amount:** `10`
4. **Next** → alert conditions:
   - Actual, **50%**
   - Actual, **90%**
   - Forecasted, **100%**
5. **Alert recipients:** your email → **Create**.

A budget **emails** you; it doesn't stop anything by itself.

## 5.3 — Create a resource group

1. Search **Resource groups** → **+ Create**.
2. **Resource group:** `rg-kafka-store` · **Region:** `Australia East`
3. **Review + create** → **Create**.

## 5.4 — Create the storage account (ADLS Gen2)

Search **Storage accounts** → **+ Create**.

**Basics tab**

| Field | Value |
|---|---|
| Resource group | `rg-kafka-store` |
| Storage account name | e.g. `stkafkastoresubs01` (3–24 chars, **lowercase letters and numbers only**, must be unique in Azure; add digits if taken) |
| Region | `Australia East` |
| Preferred storage type | **Azure Blob Storage or Azure Data Lake Storage Gen 2** |
| Performance | **Standard** |
| Redundancy | **Locally-redundant storage (LRS)** |

**Advanced tab**

| Setting | Value |
|---|---|
| Require secure transfer | ✅ On |
| Allow anonymous access on individual containers | ❌ **Off** |
| Enable storage account key access | ✅ On (needed to sign the SAS token) |
| Minimum TLS version | 1.2 |
| **Enable hierarchical namespace** | ✅ **ON — this is what makes it ADLS Gen2** |
| Access tier | **Hot** |

> ⚠️ Double-check **hierarchical namespace** before you click Create. It can't be switched off later.

**Networking:** *Enable public access from all networks* (every request still needs your SAS token).
**Data protection / Encryption / Tags:** leave defaults (optional tag `project = kafka-store`).

**Review + create** → wait for *Validation passed* → **Create** → **Go to resource**.

**Check:** on the **Overview** page, **Hierarchical namespace** = **Enabled**.

📝 Write down the **storage account name**. It's your `AZURE_STORAGE_ACCOUNT`.

## 5.5 — Create the `landing` container

1. Storage account left menu → **Data storage** → **Containers**.
2. **+ Container** → **Name:** `landing` → **Anonymous access level:** *Private* → **Create**.

The uploader creates the `raw/<topic>/<date>/` folders by itself.

## 5.6 — Create a SAS token for the uploader

1. **Containers** → click `landing`.
2. Container left menu → **Settings** → **Shared access tokens**.
3. Fill in:

| Field | Value |
|---|---|
| Signing method | **Account key** |
| Signing key | Key 1 |
| Stored access policy | None |
| Permissions | ✅ **Read** ✅ **Add** ✅ **Create** ✅ **Write** ✅ **List** (leave **Delete** unticked) |
| Start | now |
| Expiry | e.g. 30 days from today (📅 put a reminder in your calendar) |
| Allowed protocols | **HTTPS only** |

4. **Generate SAS token and URL**.
5. Copy the **Blob SAS token** (starts with `sp=racwl&st=...`). This is your `AZURE_STORAGE_SAS_TOKEN`.

> ⚠️ Treat it like a password. It only goes in `.env`.

---

---

# PART 6 — Configure V5 and Test the Connection

---

## 6.1 — Edit `.env`

Open `.env` in the V5 project and change **only these lines** (leave everything else as it is):

```env
LAKE_SINKS=volume,adls

DATABRICKS_VOLUME_PATH=/Volumes/workspace/kafka_store_v5/landing

AZURE_STORAGE_ACCOUNT=stkafkastoresubs01
AZURE_STORAGE_CONTAINER=landing
AZURE_STORAGE_SAS_TOKEN='sp=racwl&st=...&se=...&spr=https&sv=...&sr=c&sig=...'
AZURE_STORAGE_ACCOUNT_KEY=
```

- `AZURE_STORAGE_ACCOUNT`: **your** storage account name from 5.4 (just the name, not a URL).
- `AZURE_STORAGE_SAS_TOKEN`: paste the token from 5.6 **inside single quotes**.
- Keep `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as they are.

## 6.2 — Edit `lake_config.py` (line 45)

Change the fallback so V5 can never upload into V4's volume by accident:

```python
DATABRICKS_VOLUME_PATH = os.getenv(
    'DATABRICKS_VOLUME_PATH', '/Volumes/workspace/kafka_store_v5/landing')
```

## 6.3 — Test both destinations

```powershell
python check_lake_setup.py
```

Expected:

```
Testing destinations: volume, adls

✅ volume: uploaded /Volumes/workspace/kafka_store_v5/landing/_healthcheck/ping.txt
✅ adls: uploaded abfss://landing@stkafkastoresubs01.dfs.core.windows.net/_healthcheck/ping.txt

All good — you can start the lake services.
```

If you see ❌, check **Part 11 — Common Errors**.

---

---

# PART 7 — Databricks: Clone the Job for V5

---

The notebooks are **not** changed or copied. They read `catalog` and `schema` from job parameters, so a V5 job just passes `kafka_store_v5`.

1. Databricks → **Jobs & Pipelines** → open `kafka-store-medallion` (the V4 job).
2. Top right **⋮** → **Clone job**.
3. Rename the clone to `kafka-store-medallion-v5`.
4. **Job parameters** (right panel) → **Edit parameters**:

| Key | Value |
|---|---|
| `catalog` | `workspace` |
| `schema` | **`kafka_store_v5`** |

5. **Schedules & Triggers** → edit (or add) the **File arrival** trigger:
   - **Storage location:** `/Volumes/workspace/kafka_store_v5/landing/raw/`
   - **Advanced:** minimum time between triggers `300`, wait after last change `60`
   - **Trigger status:** Active → **Save**
6. Check the three tasks (`bronze` → `silver` → `gold`) still point at the notebooks in your `kafka-store-pipeline` folder. Leave them as they are.

> ✅ Make sure the **V4** job's trigger is still **Paused** (Part 1, step 3). The V4 job keeps `schema = kafka_store`, so it never touches V5 data.

---

---

# PART 8 — End-to-End Test

---

## 8.1 — Start everything (one terminal each, `.venv` activated, inside the V5 folder)

```
1.  Docker Desktop running, `docker compose up -d` done (Part 3)
2.  python check_lake_setup.py              (sanity check)
3.  python services/email_service.py        (Terminal 1)
4.  python services/stock_service.py        (Terminal 2)
5.  python services/invoice_service.py      (Terminal 3)
6.  python services/shipping_service.py     (Terminal 4)
7.  python services/tracking_service.py     (Terminal 5)
8.  python services/delivery_service.py     (Terminal 6)
9.  python app.py                           (Terminal 7)
10. python services/lake_writer.py          (Terminal 8)
11. python services/lake_uploader.py        (Terminal 9)
12. Open http://localhost:5000
```

## 8.2 — Place orders

Register a new user, then place **3–5 orders** (Stripe test card `4242 4242 4242 4242`, any future date, any CVC).

## 8.3 — Watch the uploader send each file to BOTH places

Terminal 9 should show **two lines per file**:

```
  ☁️  [volume] /Volumes/workspace/kafka_store_v5/landing/raw/order-placed/2026-10-01/batch_....parquet
  ☁️  [adls] abfss://landing@stkafkastoresubs01.dfs.core.windows.net/raw/order-placed/2026-10-01/batch_....parquet
```

## 8.4 — See your files in Azure

Azure portal → your storage account → **Storage browser** → **Blob containers** → `landing` → `raw` → `order-placed` → today's date → your `.parquet` files. 🎉

## 8.5 — Check the Databricks job and tables

About 2 minutes after the last upload: **Jobs & Pipelines** → `kafka-store-medallion-v5` → **Runs**. A run should start on its own (or click **Run now**).

Then in the **SQL Editor**:

```sql
USE CATALOG workspace;
USE SCHEMA kafka_store_v5;

SHOW TABLES;

SELECT * FROM gold_order_journey ORDER BY ordered_at DESC;
SELECT * FROM gold_fulfilment_kpis;
SELECT event_key, _source_file, _loaded_at FROM bronze_order_placed ORDER BY _loaded_at DESC;
```

Each `_source_file` path should contain `kafka_store_v5/landing/raw/...`.

Wait ~90 seconds after an order for `item-delivered`; the next run updates `current_status` to `delivered`.

## 8.6 — Prove V4 wasn't touched

```sql
SELECT COUNT(*) FROM workspace.kafka_store.bronze_order_placed;     -- V4: same as before
SELECT COUNT(*) FROM workspace.kafka_store_v5.bronze_order_placed;  -- V5: your new orders
```

---

---

# PART 9 — Save V5 to GitHub

---

1. Update `README.md`: change the title to **My Kafka Store V5** and add a short "What's new in V5 — Path B: files also land in Azure Data Lake Storage Gen2".
2. Commit and push (inside the V5 folder):

```powershell
git status
```

Make sure `.env`, `.venv`, `data-lake/` and `.idea` are **not** listed. Then:

```powershell
git add lake_config.py README.md
git commit -m "v5: Path B - upload to Databricks volume and Azure Data Lake (kafka_store_v5)"
git push

git tag -a v5.0 -m "Path B complete"
git push origin v5.0
```

3. Search for secrets before you finish:

```powershell
git grep -n "dapi"
git grep -n "sig="
```

Neither should return a real token (only placeholders or variable names). If one does, move it into `.env`, and revoke/regenerate that token.

---

---

# PART 10 — Shutdown and Cost Safety

---

**Shutdown order**

1. `Ctrl+C` in `lake_writer.py` first (it flushes and commits).
2. Wait ~15s, then `Ctrl+C` in `lake_uploader.py`.
3. `Ctrl+C` in the other services and Flask.
4. `docker compose down` (**no** `-v`).
5. Pause the `kafka-store-medallion-v5` trigger when you're not working on the project.

**Azure**

- Idle storage costs almost nothing. Keep the budget alert on.
- The SAS token expires on the date you chose. When the uploader shows `AuthorizationFailure`, generate a new one (5.6) and update `.env`.
- To remove everything: **Resource groups** → `rg-kafka-store` → **Delete resource group**. ⚠️ Don't do this if you plan to do **Path C (V6)**, which reads this same storage.

---

---

# PART 11 — Common Errors (Path B)

---

| What you see | Why | Fix |
|---|---|---|
| `NotFound: ... /Volumes/workspace/kafka_store_v5/landing` | Schema/volume not created, or typo in `.env` | Rerun Part 4. Check `DATABRICKS_VOLUME_PATH` |
| `TimeoutError` reaching `*.cloud.databricks.com` | Network/VPN/firewall, or wrong `DATABRICKS_HOST` | Check the host matches your browser URL; `Test-NetConnection <host> -Port 443` |
| `PermissionDenied` / `401` from Databricks | PAT expired or revoked | Generate a new PAT (Settings → Developer → Access tokens) |
| `ValueError: AZURE_STORAGE_ACCOUNT is empty` | Missing in `.env` | Add your storage account name |
| `AuthorizationFailure` / `AuthenticationFailed` | SAS expired, missing permission, or pasted without quotes | New SAS with Read+Add+Create+Write+List, wrapped in `'single quotes'` |
| `AuthorizationPermissionMismatch` | SAS missing **List** or **Write** | Regenerate the SAS with all 5 permissions |
| `ResourceNotFoundError: The specified filesystem does not exist` | Container `landing` missing or misspelled | Create it (5.5). Check `AZURE_STORAGE_CONTAINER` |
| `Name or service not known ... dfs.core.windows.net` | Typo in account name, or you pasted a URL | Use just the name, e.g. `stkafkastoresubs01` |
| Files pile up in `data-lake/landing/` | Uploader stopped, or one destination failing | A file only moves to `archive/` after **both** uploads succeed. Read the ❌ lines in Terminal 9 |
| V5 job never runs | Trigger paused, or path still points at `kafka_store` | Trigger path must be `/Volumes/workspace/kafka_store_v5/landing/raw/`, status Active |
| New tables appear in `kafka_store` instead of `kafka_store_v5` | Job parameter `schema` not changed | Set `schema = kafka_store_v5` on the V5 job (Part 7) |
| Port already in use / container name conflict | V4's containers still running | `docker compose down` in the V4 folder first |
| Login fails in the store | V5 has a fresh database | Register a new user |

---

---

# What's Next — V6 (Path C: Azure Databricks)

---

When V5 works end to end:

1. Tag V5 (`v5.0`, Part 9) and `docker compose down`.
2. Create V6 the same way as Part 2: new empty repo `my-kafka-store-v6`, clone **V5** into `test-v6\my-kafka-store-v6`, rename `origin` → `v5`, add the V6 `origin`, push, new `.venv`, copy `.env`.
3. Follow **Part 10 of the V4 guide** (Azure Databricks). Read its cost section first: Path C needs a pay-as-you-go subscription.
4. In V6's `.env` you'll set `LAKE_SINKS=adls`, and Azure Databricks reads the same `landing` container you built in Part 5.
