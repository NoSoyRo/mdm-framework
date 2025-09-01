"""
Command-line interface for generating COVID-19 dummy data
"""
import argparse
import os
from .covid_graph_generator import CovidGraphGenerator
from .covid_graph_querier import CovidGraphQuerier

def main():
    parser = argparse.ArgumentParser(
        description='Generate dummy COVID-19 data in Neo4j'
    )
    parser.add_argument(
        '--uri', 
        default=os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
        help='Neo4j connection URI'
    )
    parser.add_argument(
        '--user', 
        default=os.getenv('NEO4J_USER', 'neo4j'),
        help='Neo4j username'
    )
    parser.add_argument(
        '--password', 
        default=os.getenv('NEO4J_PASSWORD'),
        help='Neo4j password'
    )
    parser.add_argument(
        '--num-patients', 
        type=int, 
        default=1000,
        help='Number of patient nodes to generate'
    )
    parser.add_argument(
        '--stats', 
        action='store_true',
        help='Show statistics after generation'
    )

    args = parser.parse_args()

    if not args.password:
        raise ValueError(
            "Neo4j password must be provided via --password or NEO4J_PASSWORD env var"
        )

    # Generate data
    generator = CovidGraphGenerator(
        uri=args.uri,
        user=args.user,
        password=args.password
    )
    
    try:
        print(f"Generating {args.num_patients} patient records...")
        generator.create_covid_data(args.num_patients)
        print("Data generation complete!")

        if args.stats:
            querier = CovidGraphQuerier(
                uri=args.uri,
                user=args.user,
                password=args.password
            )
            
            try:
                # Show statistics
                stats = querier.get_general_statistics()
                print("\n=== General Statistics ===")
                print(f"Total patients: {stats['total_patients']}")
                print(f"Average age: {stats['avg_age']:.2f}")
                print(f"Variants present: {', '.join(stats['variants'])}")

                print("\n=== Hospital Distribution ===")
                for record in querier.get_hospital_distribution():
                    print(f"{record['hospital']}: {record['patients']} patients")

                dup_stats = querier.get_duplicate_statistics()
                print(f"\nPotential duplicate pairs: {dup_stats['duplicate_pairs']}")

            finally:
                querier.close()
    
    finally:
        generator.close()

if __name__ == "__main__":
    main()
