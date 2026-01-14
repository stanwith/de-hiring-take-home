# Hometask - ETL Pipeline

## Getting Started

Please follow the [fork and pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) workflow:

1. Fork the repository.
2. Create a new branch for your solution.
3. Create your pipeline code, documentation.
   - Use `uv` to manage the project and your submission is expected to include `pyproject.toml` and `uv.lock`.
   - Use python `3.13+`.
   - Follow requirements stated below.
4. Send a pull request.
   - In your PR description, include any notes about your approach, assumptions, or design decisions

## Question: Build a Data Integration Pipeline

### Context

You need to build a data pipeline that:

1. Fetches data from [Wikipedia - Toronto](https://en.wikipedia.org/wiki/Toronto) and recursively follows all links and sublinks (and their sublinks, etc.)
2. Transforms and validates the data
3. Loads it into a staging area
4. Applies data quality checks
5. Moves clean data to a final destination
6. Includes monitoring and error handling

## Requirements

### Part 1: Data Extraction

- Create a script that fetches data from [Wikipedia - Toronto](https://en.wikipedia.org/wiki/Toronto) and recursively follows all links and sublinks (and their sublinks, etc.)
- Handle rate limiting and retries
- Implement proper error handling
- Consider depth limits and circular reference handling for link traversal

### Part 2: Data Transformation

- Clean and transform the data (handle nulls, format dates, validate schemas)
- Apply business rules/logic
- Create a data quality report

### Part 3: Data Loading

- Load data to a staging location (can be CSV, JSON, or a local database)
- Implement idempotency (handle re-runs safely)
- Create a final "production" table/view

### Part 4: Monitoring & Documentation

- Add logging throughout the pipeline
- Create a README with setup instructions
- Document data schema and transformations
- Include a simple monitoring/metrics output (success rate, record counts, etc.)

### Tech Stack (use what you're comfortable with)

- Python (required)
- SQL (if using a database)
- Any libraries you prefer (pandas, sqlalchemy, requests, etc.)

## Deliverables

Your pull request should include:

1. **Working, tested code** with all code files
2. **Project configuration files:**
   - `pyproject.toml` (managed with `uv`)
   - `uv.lock`
3. **README.md** with:
   - Setup instructions
   - How to run the pipeline
   - Data schema documentation
   - Assumptions and design decisions
4. **Sample output/data** demonstrating the pipeline execution
5. **PR description** with a brief explanation of your approach

## Scaling Considerations

Your solution should be designed with scalability in mind. Consider the following production scenario:

- **Scale:** Processing data for 100,000 creators
- **Refresh frequency:** Daily updates
- **Throughput requirement:** 1+ data entries extracted/transformed/validated per second
- **Availability:** 24/7 uninterrupted operation

### Scaling Requirements

- **Document your extraction speed:** What throughput (records/second) can your solution achieve?
- **Explain your scaling approach:** How have you designed your pipeline to handle the scale requirements?
  - Consider: parallelization, batching, resource management, error recovery
  - Discuss bottlenecks and how you've addressed or would address them
  - Include any relevant metrics or benchmarks in your PR description

While you don't need to process 100k records for this assignment, your code should demonstrate the architectural patterns and design decisions that would enable scaling to this level.
