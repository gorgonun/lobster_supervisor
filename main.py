import pandas


def main(expenses_path="despesas_pagas_candidatos_2022_BRASIL.csv", candidates_path="consulta_cand_2022_BRASIL.csv"):
    expenses_df = pandas.read_csv(expenses_path, sep=';', encoding="latin-1")
    candidates_df = pandas.read_csv(candidates_path, sep=';', encoding="latin-1")

    dim_candidate = candidates_df[[
        "SQ_CANDIDATO", "DS_CARGO", "NR_CANDIDATO", "NM_CANDIDATO", "SG_PARTIDO", "DS_GENERO", "DS_COR_RACA", "SG_UF", "NM_UE"
    ]]

    dim_election = candidates_df[[
        "CD_ELEICAO", "CD_TIPO_ELEICAO", "DS_ELEICAO", "NR_TURNO"
    ]]

    dim_expenses = expenses_df[[
        "DS_FONTE_DESPESA", "DS_ORIGEM_DESPESA", "DS_NATUREZA_DESPESA"
    ]]

    dim_pc = expenses_df[[
        "TP_PRESTACAO_CONTAS", "SG_UF", "NR_DOCUMENTO"
    ]]

    dim_candidate.to_parquet("dim_candidate.parquet")
    dim_election.to_parquet("dim_election.parquet")
    dim_expenses.to_parquet("dim_expenses.parquet")
    dim_pc.to_parquet("dim_pc.parquet")


if __name__ == "__main__":
    main()
