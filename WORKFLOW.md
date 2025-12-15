# GitHub Branch Management Workflow: Reddit Data Engineering Pipeline

This document outlines the branching strategy and workflow for building the Reddit Data Pipeline project from scratch. Following this structure ensures code stability, separates concerns, and keeps credentials secure.

## 1. Branching Strategy

We follow a simplified **Git Flow** strategy.

### Core Branches
* **`main`**: The production-ready code. This branch should always be stable and deployable.
* **`develop`**: The integration branch. Features are merged here first to ensure they work together before moving to `main`.

### Temporary Branches
* **`feature/<name>`**: For new capabilities (e.g., `feature/reddit-api`, `feature/aws-s3`).
* **`chore/<name>`**: For maintenance, setup, or config tasks (e.g., `chore/init-repo`, `chore/docker-setup`).
* **`fix/<name>`**: For bug fixes (e.g., `fix/airflow-connection`).

---

## 2. Development Roadmap (Phases)

Since you are building from scratch, create these branches sequentially to maintain focus.

### Phase 1: Initialization
* **Branch:** `chore/init-project`
* **Focus:** Repo setup and environment.
* **Checklist:**
    * [ ] Initialize Git repository.
    * [ ] Create `.gitignore` (See Section 4).
    * [ ] Create `requirements.txt`.
    * [ ] Create `README.md`.

### Phase 2: Infrastructure (Docker & Airflow)
* **Branch:** `feature/docker-airflow`
* **Focus:** Getting the local orchestration running.
* **Checklist:**
    * [ ] Create `docker-compose.yml`.
    * [ ] Create `Dockerfile` (if custom extension needed).
    * [ ] Create `airflow.env` (add to .gitignore!).
    * [ ] **Success Criteria:** Airflow UI accessible at `localhost:8080`.

### Phase 3: Extraction Logic (Reddit API)
* **Branch:** `feature/reddit-extraction`
* **Focus:** Python logic to talk to Reddit.
* **Checklist:**
    * [ ] Create `config/config.conf` structure.
    * [ ] Implement connection logic in `utils/`.
    * [ ] Write extraction script in `etls/reddit_etl.py`.
    * [ ] **Success Criteria:** Script runs locally and prints Reddit data to console.

### Phase 4: Storage Integration (AWS S3)
* **Branch:** `feature/aws-s3`
* **Focus:** Moving data from local to cloud.
* **Checklist:**
    * [ ] Add S3 connection helper in `utils/`.
    * [ ] Update ETL script to upload to S3 bucket.
    * [ ] **Success Criteria:** Running the script creates a file in your S3 bucket.

### Phase 5: Orchestration (DAGs)
* **Branch:** `feature/airflow-dag`
* **Focus:** Automating the run.
* **Checklist:**
    * [ ] Create `dags/reddit_dag.py`.
    * [ ] Define task dependencies (Extract -> Load).
    * [ ] **Success Criteria:** Triggering the DAG in Airflow UI runs the whole pipeline successfully.

### Phase 6: Transformation & Warehousing (Glue/Redshift)
* **Branch:** `feature/aws-glue-redshift`
* **Focus:** Final data modeling.
* **Checklist:**
    * [ ] Write Glue transformation scripts.
    * [ ] Configure Redshift connection.
    * [ ] Add Glue/Redshift operators to the Airflow DAG.

---

## 3. Workflow Steps

1.  **Start a new feature:**
    ```bash
    git checkout develop
    git pull origin develop
    git checkout -b feature/name-of-task
    ```

2.  **Work and Commit:**
    ```bash
    git add .
    git commit -m "feat: description of what you built"
    ```

3.  **Push and Merge:**
    ```bash
    git push origin feature/name-of-task
    ```
    * *Action:* Go to GitHub, open a **Pull Request (PR)** from `feature/...` to `develop`.
    * *Action:* Merge the PR after review.

4.  **Cleanup:**
    ```bash
    git checkout develop
    git pull origin develop
    git branch -d feature/name-of-task
    ```

---

## 4. Security & Guardrails

**CRITICAL:** Never commit credentials. Ensure your `.gitignore` includes:

```text
# .gitignore
__pycache__/
*.pyc
venv/
.env
*.env
config.conf
config/config.conf
credentials/
.DS_Store
logs/