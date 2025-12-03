# Kaelynn lackey
# 1 December 2025
import click
from pipeline import etl_pipeline
import json

# cases: fetch, state, summary, timeline, top

@click.group()
def cli():
    """covid data ETL pipeline"""
    pass

@cli.command()
@click.option('--state', help='State code (e.g., CA, NY)')
@click.option('--all-states', is_flag=True, help='Fetch all states')
@click.option('--limit', default=None, type=int, help='Limit number of states')
def fetch(state, all_states, limit):
    pipeline=etl_pipeline()
    if all_states:
        total = pipeline.run_for_all_states(limit=100)
        click.echo(f"loaded {total} records across all states")
    elif state:
        records = pipeline.run_for_state(state)
        click.echo(f"loaded {len(records)} records for {state}")
    else:
        click.echo("error: Specify --state CODE or --all-states")


@cli.command()
@click.option('--limit',default=10, help='number of results')
@click.option('--metric', default='cases', 
              type=click.Choice(['cases', 'deaths']))
def top(limit, metric):
    pipeline =etl_pipeline()
    if metric == 'cases':
        results =pipeline.query_top_cases(limit)
    else:
        results=pipeline.query_top_deaths(limit)
    click.echo(json.dumps(results, indent=2, default=str))


@cli.command()
@click.argument('state')
def state(state):
    pipeline=etl_pipeline()
    result = pipeline.query_state(state.upper())
    if result:
        click.echo(json.dumps(result,indent=2, default=str))
    else:
        click.echo(f"no data found for {state}")


@cli.command()
@click.argument('state')
@click.option('--days', default=30, help='Number of days')
def timeline(state, days):
    pipeline=etl_pipeline()
    results= pipeline.query_time_series(state.upper(), days)
    click.echo(f"\nlast {days} days for {state}:")
    click.echo(json.dumps(results, indent=2, default=str))

@cli.command()
def summary():
    """Get summary statistics"""
    pipeline=etl_pipeline()
    stats = pipeline.get_summary()
    click.echo(f"total states: {stats.get('total_states') or 0}")
    click.echo(f"total cases: {(stats.get('total_cases') or 0):,}")
    click.echo(f"total deaths: {(stats.get('total_deaths') or 0):,}")
    hospitalized = stats.get('total_hospitalized')
    click.echo(f"currently hospitalized: {(hospitalized or 0):,}")
    click.echo(f"latest data date: {stats.get('latest_date')}")

@cli.command()
@click.argument('state')
@click.option('--metric', default='cases',
              type=click.Choice(['cases', 'deaths']))
@click.option('--days', default=30, help='Number of days to plot')
@click.option('--output', default='timeline.png',
              help='Output file for the chart')
def visualize(state, metric, days, output):
    """Generate a simple timeline visualization for a state."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    pipeline = etl_pipeline()
    records = pipeline.query_time_series(state.upper(), days)
    if not records:
        click.echo(f"no data found for {state}")
        return

    sorted_records = sorted(records, key=lambda r: r['date'])
    dates = [r['date'] for r in sorted_records]
    values = [r['cases_total'] if metric == 'cases' else r['deaths_total']
              for r in sorted_records]

    plt.figure(figsize=(10, 5))
    plt.plot(dates, values, marker='o')
    plt.title(f"{state.upper()} {metric} over last {len(sorted_records)} days")
    plt.xlabel('Date')
    plt.ylabel('Total ' + metric)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.grid(True)
    plt.savefig(output)
    click.echo(f"saved visualization to {output}")

if __name__ == '__main__':
    cli()


