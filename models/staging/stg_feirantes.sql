with
    source as 
        (
            select *
            from {{ source('raw', 'feirantes') }}
    )
    
    , cleaned as (
        select
            serial bigint as serial_key
            , wave_date date
            , main_activity text
            , aided_feira_da_parangaba boolean
            , aided_feirão_de_baturité boolean
            , ever_used_feira_da_parangaba boolean
            , ever_used_feirão_de_baturité boolean
            , preference_feira_da_parangaba boolean
            , preference_feirão_de_baturité boolean
            , toma_feira_da_parangaba boolean
            , toma_feirão_de_baturité boolean
            , used_p1m_feira_da_parangaba boolean
            , used_p1m_feirão_de_baturité boolean
            , dual_feiras__trabalhou_no_past_1_month_nas_duas_fe text
            , behavior text
            , i_am_proud_to_work_for_this_market_feira_da_parang boolean
            , i_am_proud_to_work_for_this_market_feirão_de_batur boolean
            , it_is_near_from_my_house_feira_da_parangaba boolean
            , it_is_near_from_my_house_feirão_de_baturité boolean
            , i_feel_comfortable_with_the_clients_of_this_market_19 boolean
            , i_feel_comfortable_with_the_clients_of_this_market_20 boolean
            , i_rarely_have_clients_that_makes_me_feel_unsafe_fe_21 boolean
            , i_rarely_have_clients_that_makes_me_feel_unsafe_fe_22 boolean
            , it_allows_me_to_have_total_control_of_my_gains_fei_23 boolean
            , it_allows_me_to_have_total_control_of_my_gains_fei_24 boolean
            , it_cares_about_society_feira_da_parangaba boolean
            , it_cares_about_society_feirão_de_baturité boolean
            , it_cares_about_their_vendors_gains_feira_da_parang boolean
            , it_cares_about_their_vendors_gains_feirão_de_batur boolean
            , it_cares_about_their_vendors_feira_da_parangaba boolean
            , it_cares_about_their_vendors_feirão_de_baturité boolean
            , it_has_security_all_over_the_market_feira_da_paran boolean
            , it_has_security_all_over_the_market_feirão_de_batu boolean
            , it_has_a_lower_fee_than_the_others_feira_da_parang boolean
            , it_has_a_lower_fee_than_the_others_feirão_de_batur boolean
            , it_has_an_agile_quick_witted_channel_for_suggestio_35 boolean
            , it_has_an_agile_quick_witted_channel_for_suggestio_36 boolean
            , it_has_better_selection_of_clients_feira_da_parang boolean
            , it_has_better_selection_of_clients_feirão_de_batur boolean
            , it_has_better_vendors_selection_criteria_feira_da_ boolean
            , it_has_better_vendors_selection_criteria_feirão_de boolean
            , it_has_communications_that_mean_something_to_me_fe_41 boolean
            , it_has_communications_that_mean_something_to_me_fe_42 boolean
            , it_has_more_clients_than_others_feira_da_parangaba boolean
            , it_has_more_clients_than_others_feirão_de_baturité boolean
            , it_has_premium_spots_that_helps_to_increasy_my_ear_45 boolean
            , it_has_premium_spots_that_helps_to_increasy_my_ear_46 boolean
            , it_has_several_options_for_paying_my_fees_feira_da boolean
            , it_has_several_options_for_paying_my_fees_feirão_d boolean
            , it_has_the_best_benefit_programs_feira_da_parangab boolean
            , it_has_the_best_benefit_programs_feirão_de_baturit boolean
            , it_helps_me_understand_what_i_have_to_do_when_im_e_51 boolean
            , it_helps_me_understand_what_i_have_to_do_when_im_e_52 boolean
            , it_is_a_trusted_market_feira_da_parangaba boolean
            , it_is_a_trusted_market_feirão_de_baturité boolean
            , it_is_an_easy_to_work_market_feira_da_parangaba boolean
            , it_is_an_easy_to_work_market_feirão_de_baturité boolean
            , it_is_concerned_care_about_physical_safety_of_its__57 boolean
            , it_is_concerned_care_about_physical_safety_of_its__58 boolean
            , it_is_the_fastest_at_paying_my_earnings_feira_da_p boolean
            , it_is_the_fastest_at_paying_my_earnings_feirão_de_ boolean
            , it_makes_me_feel_safe_feira_da_parangaba boolean
            , it_makes_me_feel_safe_feirão_de_baturité boolean
            , it_offers_a_good_distance_from_the_others_vendors__63 boolean
            , it_offers_a_good_distance_from_the_others_vendors__64 boolean
            , it_offers_better_structure_of_market_stall_feira_d boolean
            , it_offers_better_structure_of_market_stall_feirão_ boolean
            , it_offers_financial_incentives_to_increase_my_earn_67 boolean
            , it_offers_financial_incentives_to_increase_my_earn_68 boolean
            , it_offers_many_benefits_and_partnerships_to_their__69 boolean
            , it_offers_many_benefits_and_partnerships_to_their__70 boolean
            , it_offers_multiple_ways_to_charge_feira_da_paranga boolean
            , it_offers_multiple_ways_to_charge_feirão_de_baturi boolean
            , it_offers_the_best_cost_benefit_feira_da_parangaba boolean
            , it_offers_the_best_cost_benefit_feirão_de_baturité boolean
            , it_offers_total_transparency_of_my_gains_feira_da_ boolean
            , it_offers_total_transparency_of_my_gains_feirão_de boolean
            , it_provides_me_clients_all_the_time_feira_da_paran boolean
            , it_provides_me_clients_all_the_time_feirão_de_batu boolean
            , its_a_brand_that_offers_the_option_to_negotiate_th_79 boolean
            , its_a_brand_that_offers_the_option_to_negotiate_th_80 boolean
            , the_market_uses_modern_structure_feira_da_parangab boolean
            , the_market_uses_modern_structure_feirão_de_baturit boolean
            , the_rules_of_the_market_are_fair_feira_da_parangab boolean
            , the_rules_of_the_market_are_fair_feirão_de_baturit boolean
        from source
    )

select *
from cleaned
