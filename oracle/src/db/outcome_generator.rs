use itertools::Itertools;
use rayon::prelude::*;

pub fn generate_winner_permutations(num_players: usize) -> Vec<Vec<usize>> {
    (0..=num_players)
        .into_par_iter()
        .flat_map(|r| {
            (0..num_players)
                .combinations(r)
                .map(|v| v.into_iter().collect::<Vec<_>>())
                .collect::<Vec<_>>()
        })
        .collect()
}

pub fn generate_outcome_messages(possible_user_outcomes: Vec<Vec<usize>>) -> Vec<Vec<u8>> {
    possible_user_outcomes
        .into_iter()
        .map(|inner_vec| {
            inner_vec
                .into_iter()
                .flat_map(|num| num.to_be_bytes())
                .collect::<Vec<u8>>()
        })
        .collect()
}

#[cfg(test)]
mod test {

    use super::generate_winner_permutations;

    #[test]
    fn can_generate_list_of_winners_small() {
        let num_players = 5;
        let permutations: Vec<Vec<usize>> = generate_winner_permutations(num_players);
        println!("permutations: {:?}", permutations);
        assert_eq!(permutations.len(), 32);
    }

    #[test]
    fn can_generate_list_of_winners_default_size() {
        let num_players = 20;
        let permutations: Vec<Vec<usize>> = generate_winner_permutations(num_players);
        assert_eq!(permutations.len(), 1_048_576);
    }

    #[test]
    fn can_generate_list_of_winners_large() {
        let num_players = 25;
        let permutations: Vec<Vec<usize>> = generate_winner_permutations(num_players);
        assert_eq!(permutations.len(), 33_554_432);
    }
}
