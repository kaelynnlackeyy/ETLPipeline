# Kaelynn lackey
# 1 December 2025
import click
from pipeline import etl_pipeline
import json

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
@click.argument('state_code')
def state(state_code):
    pipeline=etl_pipeline()
    result = pipeline.query_state(state_code)
    if result:
        click.echo(json.dumps(result,indent=2, default=str))
    else:
        click.echo(f"no data found for {state_code}")


@cli.command()
@click.argument('state_code')
@click.option('--days', default=30, help='Number of days')
def timeline(state_code, days):
    pipeline=etl_pipeline()
    results= pipeline.query_time_series(state_code, days)
    click.echo(f"\nlast {days} days for {state_code}:")
    click.echo(json.dumps(results, indent=2, default=str))

@cli.command()
def summary():
    """Get summary statistics"""
    pipeline=etl_pipeline()
    stats = pipeline.get_summary()
    click.echo("\n=== covid-19 summary statistics ===")
    click.echo(f"total states: {stats.get('total_states')}")
    click.echo(f"total cases: {stats.get('total_cases'):,}")
    click.echo(f"total deaths: {stats.get('total_deaths'):,}")
    click.echo(f"currently Hhspitalized: {stats.get('total_hospitalized'):,}")
    click.echo(f"latest data date: {stats.get('latest_date')}")

if __name__ == '__main__':
    cli()


