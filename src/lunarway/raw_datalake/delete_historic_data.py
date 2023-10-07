
import boto3
from lunarway.hour_partition import parse_hour_partition
from datetime import date

# session = boto3.Session(profile_name='deploy')
# s3c = boto3.client("s3", region_name='eu-west-1')

s3 = boto3.resource('s3')


bucket = s3.Bucket('lunarway-dev-data-raw-datalake')
source = 'integrationevents'


def delete_data(topic, until):
    prefix = f"source={source}/topic={topic}"
    # print(f"Deleting objects in: {prefix}")
    for obj in bucket.objects.filter(Prefix=prefix):
        partition = parse_hour_partition(obj.key)

        if partition.date == until and partition.hour < 15:
            print(f"deleting: {obj.key}")
            # obj.delete()

#
topics = [
    'account_block.account.AccountBlockStatusUpdated',
    'account_block.account.AccountBlockStatusUpdatedV2',
    'account_management.account.AccessAdded',
    'account_management.account.AccessRemoved',
    'account_management.account.AccountClosed',
    'account_management.account.AccountCreated',
    'account_management.account.AccountCreatedV2',
    'account_management.account.PrimaryAccountSetForOwner',
    'account_migration.account.AccountMigrated',
    'account_migration.user.AllAccountsForUserMigrated',
    'account_payments.clearing_cycle.ClearingCycleReconciled',
    'account_payments.clearing_cycle.ClearingCycleReconciledV2',
    'account_payments.incoming_credit_transfer.Compensated',
    'account_payments.incoming_credit_transfer.Posted',
    'account_payments.internal_credit_transfer.Failed',
    'account_payments.internal_credit_transfer.FundsReserved',
    'account_payments.internal_credit_transfer.Posted',
    'account_payments.outgoing_credit_transfer.Compensated',
    'account_payments.outgoing_credit_transfer.Corrected',
    'account_payments.outgoing_credit_transfer.Failed',
    'account_payments.outgoing_credit_transfer.FundsReserved',
    'account_payments.outgoing_credit_transfer.IncomingCreditTransferTransactionPosted',
    'account_payments.outgoing_credit_transfer.Initiated',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferCancelled',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferExecutionStarted',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferFailed',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferFundsReserved',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferPostponedForFutureExecution',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferPostponedUpdated',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferSucceeded',
    'account_payments.outgoing_credit_transfer.OutgoingCreditTransferTransactionPosted',
    'account_payments.outgoing_credit_transfer.Posted',
    'account_payments.outgoing_credit_transfer.Reversed',
    'account_payments.outgoing_transfer_from_internal_account.Initiated',
    'account_payments.settlement.SettlementCompleted',
    'account_payments_bankgirot_clearing.incoming_correction.Cleared',
    'account_payments_bankgirot_clearing.incoming_credit_transfer.Cleared',
    'account_payments_bankgirot_clearing.incoming_reversal.Cleared',
    'account_payments_bankgirot_clearing.outgoing_correction.StatusUpdated',
    'account_payments_bankgirot_clearing.outgoing_credit_transfer.StatusUpdated',
    'account_payments_bankgirot_clearing.outgoing_reversal.StatusUpdated',
    'account_payments_bankgirot_clearing.reconciliation.ReportCreated',
    'account_payments_front.domestic_credit_transfer.OpenBankingPaymentApproved',
    'account_payments_front.domestic_credit_transfer.OpenBankingPaymentInitiated',
    'account_payments_nics_clearing.outgoing_correction.StatusUpdated',
    'account_payments_nics_clearing.outgoing_structured_credit_transfer.StatusUpdated',
    'account_payments_nics_clearing.outgoing_unstructured_credit_transfer.StatusUpdated',
    'account_payments_nics_clearing.reconciliation.ReportCreated',
    'account_payments_nics_clearing.reconciliation.ReportCreatedV2',
    'account_payments_rix_rtgs.incoming_credit_transfer.ClearedVbeta',
    'account_payments_rix_rtgs.outgoing_credit_transfer.StatusUpdated',
    'account_payments_rix_rtgs.settlement.Settled',
    'account_postings.account.AccountBalanceUpdatedV1',
    'account_postings.account.TransactionPostedV1',
    'account_postings.balance.AccountBalanceUpdatedV1',
    'account_postings.posting.AccountBalanceUpdatedV1',
    'account_postings.posting.PostingCreatedV1',
    'account_postings.posting.TransactionPostedV1',
    'accounts_view.account.PrimaryAccountUpdatedForUser',
    'add_money.topup.TopupCompleted',
    'add_money.topup.TopupStarted',
    'agreements.subsidiaryagreement.SubsidiaryAgreementAdded',
    'agreements.subsidiaryagreement.SubsidiaryAgreementRemoved',
    'analytics.currency_rate.CurrencyRateUpdated',
    'analytics.currency_rate.CurrencyRateUpdatedTest',
    'analytics.currency_rate.CurrencyRateUpdatedTestTest',
    'analytics.currency_rate.CurrencyRateUpgradedTest',
    'analytics.entitytype.eventname',
    'analytics.test2.test3',
    'bachelor_fuel.transaction.TransactionCompleted',
    'bec.credit_transfer.CreditTransferFailed',
    'bec.credit_transfer.CreditTransferSucceeded',
    'bec_reactivation.bec_reactivation.AccountAgreementAccepted',
    'bec_reactivation.bec_reactivation.ReactivationCancelled',
    'bec_reactivation.bec_reactivation.ReactivationCompleted',
    'bec_reactivation.bec_reactivation.ReactivationStarted',
    'bills.transaction.TransactionAgreementIDUpdated',
    'bills.transaction.TransactionCreated',
    'bookkeeping_accounts.bookkeeping_transaction.TransactionAssignedBookkeepingAccount',
    'card_payments.card_payment.CaptureFundsFailed',
    'card_payments.card_payment.Created',
    'card_payments.card_payment.Deleted',
    'card_payments.card_payment.FundsCaptured',
    'card_payments.card_payment.FundsReserved',
    'card_payments.card_payment.ReservationAdjusted',
    'card_payments.card_payment.ReservationCancelled',
    'card_payments.card_payment.ReservationDeclined',
    'card_utility.nets_file.FileArchived',
    'clearing.clearing_cycle.ClearingCycleEnded',
    'clearing.kid_agreement.KidAgreementUpdated',
    'clearing.outgoing_correction.PaymentClearingStatusUpdate',
    'clearing.outgoing_credit_transfer.PaymentClearingStatusUpdate',
    'clearing.outgoing_nics_kid_transfer.PaymentClearingStatusUpdate',
    'clearing.outgoingcredittransfer.PaymentClearingStatusUpdate',
    'clearing.payment.PaymentClearingStatusUpdate',
    'clearing.paymentvbeta.PaymentClearingStatusUpdate',
    'company.company.CompanyAddressUpdated',
    'company.company.CompanyCreated',
    'company.company.CompanyNameUpdated',
    'consent.consent.consentGiven',
    'consent.consent.consentGivenV2',
    'consent.consent.consentWithdrawn',
    'counterparty_screening.screening.Confirmed',
    'counterparty_screening.screening.Created',
    'counterparty_screening.screening.Rejected',
    'coworker.coworker.CoworkerApproved',
    'coworker.coworker.CoworkerCreated',
    'coworker.coworker.CoworkerPhoneNumberUpdated',
    'coworker.coworker.CoworkerRemoved',
    'credit_app_se.credit_app_se.CreditLineApplicationApprovedEvent',
    'credit_app_se.credit_app_se.CreditLineApplicationCreatedEvent',
    'credit_app_se.credit_app_se.CreditLineApplicationRejectedEvent',
    'credit_app_se.credit_app_se.CreditLineApplicationSubmittedEvent',
    'credit_app_se.credit_app_se.CreditLineOnboardingStartedEvent',
    'credit_line.credit_line.CreditLineClosedEvent',
    'credit_line.credit_line.CreditLineCreditUtiliseLevelEvent',
    'credit_line.credit_line.CreditLineOfferExpiredEvent',
    'credit_line.credit_line.CreditLineUserAcceptedOfferEvent',
    'credit_line.credit_line.CreditLineUserRejectedOfferEvent',
    'crypto.coin.CoinAdded',
    'crypto.coin.CoinUpdated',
    'crypto.market_order.MarketBuyOrderCompleted',
    'crypto.market_order.MarketSellOrderCompleted',
    'crypto.onboarding_referral.OnboardingReferralApplied',
    'crypto.onboarding_referral.OnboardingReferralBonusAwarded',
    'crypto.onboarding_referral.OnboardingReferralRedeemed',
    'crypto.recurring_trade.RecurringTradeCreated',
    'crypto.recurring_trade.RecurringTradeDeleted',
    'crypto.recurring_trade.RecurringTradeTradeCompleted',
    'crypto.transaction.TransactionEnriched',
    'crypto.user.CommunicationConsentChanged',
    'crypto.user.HoldingManuallyAdjusted',
    'crypto.user.KYCCompleted',
    'crypto.user.LiteracyTestCompleted',
    'crypto.user.LiteracyTestFailed',
    'crypto.user.LiteracyTestStarted',
    'crypto.user.ReferralBonusAwarded',
    'crypto.user.TermsAndConditionsSigned',
    'crypto.user.UserPortfolioUpdated',
    'customer_screening.list_match.ListMatchCreated',
    'customer_screening.list_match.ListMatchRejected',
    'customer_screening.screening.ScreeningCompleted',
    'customer_screening.screening.ScreeningCreated',
    'direct_debit.mandate.MandateCreateRequested',
    'direct_debit.mandate.MandateCreated',
    'direct_debit.mandate.MandateInstructionAdded',
    'direct_debit.mandate.MandateNoteUpdated',
    'document_delivery.batch.DocumentDeliveryFailed',
    'document_delivery.batch.DocumentDeliverySent',
    'domestic_credit_transfer.outgoing_credit_transfer.Failed',
    'domestic_credit_transfer.outgoing_credit_transfer.FundsReserved',
    'domestic_credit_transfer.outgoing_credit_transfer.IncomingCreditTransferTransactionPosted',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferCancelled',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferCorrected',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferExecutionStarted',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferFailed',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferFundsReserved',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferFutureCreated',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferFutureDeleted',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferPostponedForFutureExecution',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferPostponedUpdated',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferSucceeded',
    'domestic_credit_transfer.outgoing_credit_transfer.OutgoingCreditTransferTransactionPosted',
    'domestic_credit_transfer.outgoing_credit_transfer.Posted',
    'domestic_credit_transfer.outgoing_kid_transfer.OutgoingKidTransferExecutionStarted',
    'domestic_credit_transfer.outgoing_kid_transfer.OutgoingKidTransferFundsReserved',
    'domestic_credit_transfer.outgoing_kid_transfer.OutgoingKidTransferSucceeded',
    'domestic_credit_transfer.outgoing_kid_transfer.OutgoingKidTransferTransactionPosted',
    'duplicate_check.duplicate_check.DuplicateHitsByEmail',
    'duplicate_check.duplicate_check.DuplicateHitsByPhone',
    'end_user_authentication.auth.AuthApprovedByUser',
    'end_user_authentication.auth.AuthCompleted',
    'end_user_authentication.auth.AuthExpired',
    'end_user_authentication.auth.AuthProceeded',
    'end_user_authentication.auth.AuthStarted',
    'fcp_transaction_view.payment.PaymentCancelled',
    'fcp_transaction_view.payment.PaymentPosted',
    'fcp_transaction_view.payment.PaymentReserved',
    'feedback.feedback.QuestionAdded',
    'feedback.feedback.QuestionAnswered',
    'feedback.feedback.QuestionDiscarded',
    'feedback.feedback.QuestionExposed',
    'finance_manager.spend_setting.SpendSettingsChanged',
    'financial_accounting.posting.BookedBECElwoodPosting',
    'financial_accounting.posting.CryptoPostingCreated',
    'friend.friend.FriendCreated',
    'friend.friend.FriendDeleted',
    'friend.friend.FriendTransactionAdded',
    'friend.friend.FriendTransactionUpdated',
    'friend.friend.FriendUserDefinedIconURLUpdated',
    'friend.friend.FriendUserDefinedNameUpdated',
    'houston_customer.customer.CustomerMigratedFromLegacy',
    'houston_task.task.TaskApproved',
    'houston_task.task.TaskAssigned',
    'houston_task.task.TaskCompleted',
    'houston_task.task.TaskCreated',
    'houston_task.task.TaskMigrated',
    'houston_task.task.TaskRedone',
    'houston_task.task.TaskRejected',
    'houston_task.task.TaskRelationMigratedFromPrivateToPrivateParty',
    'houston_task.task.TaskTagSet',
    'houston_task.task.TaskTagsSet',
    'houston_task.task.TaskTagsUnset',
    'houston_task.task.TaskUnAssigned',
    'houston_task.task.TaskUpdated',
    'houston_tasks.task.TaskCreated',
    'identity.identity.IdentityAddressUpdated',
    'identity.identity.IdentityAgeUpdated',
    'identity.identity.IdentityBirthdateUpdated',
    'identity.identity.IdentityCreated',
    'identity.identity.IdentityEmailUpdated',
    'identity.identity.IdentityGenderUpdated',
    'identity.identity.IdentityNameUpdated',
    'identity.identity.IdentityPhoneNumberUpdated',
    'identity.identity.IdentityStatusUpdated',
    'identity.identity.UserRelationAdded',
    'identity.identity.UserRelationRemoved',
    'international_payments.incoming_transfer.Created',
    'international_payments.incoming_transfer.IncomingTransferPosted',
    'international_payments.incoming_transfer.IncomingTransferReturned',
    'international_payments.incoming_transfer.Posted',
    'international_payments.incoming_transfer.Returned',
    'international_transfer.fee.FeePosted',
    'international_transfer.fee.Posted',
    'international_transfers.swift_file.FileReceived',
    'invest.account.AccountAdded',
    'invest.account.AccountBalanceUpdated',
    'invest.account.AccountInfoUpdated',
    'invest_eod.invest_eod_event.BalanceFetched',
    'invest_eod.invest_eod_event.CashTransactionsFetched',
    'invest_eod.invest_eod_event.ClosedPositionsFetched',
    'invest_eod.invest_eod_event.DailyBalanceEvent',
    'invest_eod.invest_eod_event.DailyPositionsEvent',
    'invest_eod.invest_eod_event.DividendFetched',
    'invest_eod.invest_eod_event.OpenPositionsFetched',
    'invest_eod.invest_eod_event.TradesExecutedFetched',
    'lunar_way_bec_adapter.credit_transfer.CreditTransferFailed',
    'lunar_way_bec_adapter.credit_transfer.CreditTransferSucceeded',
    'lunarpay.payment.PaymentCaptured',
    'lunarpay.payment.PaymentRequestApproved',
    'lunarpay.payment.PaymentRequestCreated',
    'lunarpay.payment.PaymentRequestDeclined',
    'lunarpay.payment.PaymentRequestNotMatched',
    'noitso.budget.BudgetCalculated',
    'noitso.budget.BudgetCalculationFailed',
    'noitso.budget.EskatDataCollected',
    'nordea_currency.currency_rate.CurrencyRateUpdated',
    'oauth.auth.AuthAborted',
    'oauth.auth.AuthApprovedByUser',
    'oauth.auth.AuthCompleted',
    'oauth.auth.AuthExpired',
    'oauth.auth.AuthProceeded',
    'oauth.auth.AuthStarted',
    'oedd.audit.AuditApproved',
    'oedd.audit.AuditCreated',
    'oedd.gatekeeper.GatekeeperCancelled',
    'oedd.gatekeeper.ScheduleCancelled',
    'oedd.gatekeeper.ScheduleUpdated',
    'oedd.informationretrieval.InformationRetrievalCancelled',
    'oedd.informationretrieval.InformationRetrievalStarted',
    'oedd.informationretrieval.InformationRetrievalSubmitted',
    'oedd.reminder.ReminderCancelled',
    'oedd.reminder.ReminderCompleted',
    'oedd.reminder.ReminderCreated',
    'oedd.reminder.ReminderStopped',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingApprovedEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingAreasOfInterestSelectedEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingConfirmRejectEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingEvalAutoApprovaldContent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingEvaluateAutomaticApprovalEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingRedoEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingRejectEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingStartedEvent',
    'onboarding_events.consumer_onboarding.ConsumerOnboardingSubmittedEvent',
    'onboarding_events.consumer_onboarding.CreatePrivateSubscription',
    'onboarding_events.general_onboarding.OnboardingApprovedEvent',
    'onboarding_events.general_onboarding.OnboardingRedoEvent',
    'onboarding_events.general_onboarding.OnboardingRejectEvent',
    'onboarding_events.general_onboarding.OnboardingStartedEvent',
    'onboarding_events.general_onboarding.OnboardingSubmittedEvent',
    'onboarding_events.norway_business_onboarding_approved.NorwayBusinessOnboardingApprovedEvent',
    'onboarding_events.norway_business_onboarding_started.NorwayBusinessOnboardingStartedEvent',
    'onboarding_events.onboarding_approved.OnboardingApprovedEvent',
    'onboarding_events.onboarding_started.OnboardingStartedEvent',
    'onboarding_events.organisation_light_onboarding.OrganisationLightOnboardingOrganisationCreatedEvent',
    'onboarding_events.organisation_light_onboarding.OrganisationLightOnboardingOrganisationCreatedEventName',
    'onboarding_events.organisation_light_onboarding.PaymentOnboardingEmailEvent',
    'onboarding_events.organisation_onboarding.OnboardingCompaniesFetchedEvent',
    'onboarding_events.organisation_onboarding.OnboardingFilesToUploadEvent',
    'onboarding_events.organisation_onboarding.OrganisationLightOnboardingOrganisationCreatedEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnbardingRedoEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnbardingSubmittedEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingApprovedEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingCompaniesFetchedEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingRedoEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingRejectEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingStartedEvent',
    'onboarding_events.organisation_onboarding.OrganisationOnboardingSubmittedEvent',
    'onboarding_events.organisation_onboarding_approved.OrganisationOnboardingApprovedEvent',
    'onboarding_events.organisation_onboarding_started.OrganisationOnboardingStartedEvent',
    'online_payments.enrollment.EnrollmentCompleted',
    'online_payments.kyc_process.KycProcessStarted',
    'online_payments.kyc_process.KycRelatedPersonAdded',
    'online_payments.kyc_process.KycRelatedPersonRemoved',
    'online_payments.kyc_process.KycRelatedPersonUpdated',
    'online_payments.kyc_process.KycStateUpdated',
    'online_payments.merchant_created.MerchantCreated',
    'online_payments.standalone_enrollment.StandAloneEnrollmentCompleted',
    'openbanking_connect.connection_attempt.ConnectionAttemptAborted',
    'openbanking_connect.connection_attempt.ConnectionAttemptApprovedByUser',
    'openbanking_connect.connection_attempt.ConnectionAttemptCompleted',
    'openbanking_connect.connection_attempt.ConnectionAttemptExpired',
    'openbanking_connect.connection_attempt.ConnectionAttemptProceeded',
    'openbanking_connect.connection_attempt.ConnectionAttemptStarted',
    'openbanking_tpp.tpp.TPPRegisteredEvent',
    'organisation.organisation.OrganisationAddressUpdated',
    'organisation.organisation.OrganisationCreated',
    'organisation.organisation.OrganisationEmailUpdated',
    'organisation.organisation.OrganisationEstablishedDateSet',
    'organisation.organisation.OrganisationMigrated',
    'organisation.organisation.OrganisationNameUpdated',
    'organisation.organisation.OrganisationPartyActivated',
    'organisation.organisation.OrganisationPartyAdded',
    'organisation.organisation.OrganisationPartyRemoved',
    'organisation.organisation.OrganisationPartySignedUp',
    'organisation.organisation.OrganisationPartySuspensionStateUpdated',
    'organisation.organisation.OrganisationPhoneNumberUpdated',
    'organisation.organisation.OrganisationPrimaryIndustryCodeUpdated',
    'organisation.organisation.OrganisationPurposeUpdated',
    'organisation.organisation.OrganisationRegistrationDateSet',
    'organisation.organisation.OrganisationSecondaryIndustryCodesUpdated',
    'organisation.organisation.OrganisationSuspensionStateUpdated',
    'organisation.organisation.OrganisationTypeUpdated',
    'organisation.organisation_party.OrganisationPartyActivated',
    'organisation.organisation_party.OrganisationPartyClosed',
    'organisation.organisation_party.OrganisationPartyCreated',
    'organisation.organisation_party.OrganisationPartyEmailUpdated',
    'organisation.organisation_party.OrganisationPartyMigrated',
    'organisation.organisation_party.OrganisationPartyPhoneNumberUpdated',
    'organisation.organisation_party.OrganisationPartySignedUp',
    'organisation.organisation_party.OrganisationPartySuspensionStateUpdated',
    'overdraft.debtcollection.moneyTransferred',
    'overdraft.impairment.ImpairmentCreated',
    'payment_documentation.payment_documentation.PaymentDocumentationApproved',
    'payment_documentation.payment_documentation.PaymentDocumentationCreated',
    'payment_documentation.payment_documentation.PaymentDocumentationRejected',
    'paymybill.bill.PayMyBillCreated',
    'paymybill.bill.PayMyBillImageAttached',
    'paymybill.bill.PayMyBillProcessed',
    'premium.subscription.PlanCancelled',
    'premium.subscription.PlanChanged',
    'premium.subscription.PlanResumed',
    'premium.user_plan.PlanCancelled',
    'premium.user_plan.PlanChanged',
    'premium.user_plan.PlanResumed',
    'privateparty.privateparty.PrivatePartyActivated',
    'privateparty.privateparty.PrivatePartyClosed',
    'privateparty.privateparty.PrivatePartyCreated',
    'privateparty.privateparty.PrivatePartyMigratedFromLegacy',
    'privateparty.privateparty.PrivatePartySignedUp',
    'privateparty.privateparty.PrivatePartySuspensionStateUpdated',
    'privateparty.privateparty.PrivatePartySuspensionUpdated',
    'promo_codes.promo_code.PromoCodeCreated',
    'promo_codes.promo_code.PromoCodeEntered',
    'referral.referral.RedeemerActivityRegistered',
    'referral.referral.ReferralCodeVerified',
    'riskprofile.riskprofile.KYCProjectionUpdated',
    'riskprofile.riskprofile.ManualThresholdUpdated',
    'riskprofile.riskprofile.ManuallyUpdated',
    'riskprofile.riskprofile.OrganisationDataUpdated',
    'riskprofile.riskprofile.PrivatePartyDataUpdated',
    'riskprofile.riskprofile.ProfileAddedToRiskSegment',
    'riskprofile.riskprofile.ProfileRemovedFromRiskSegment',
    'riskprofile.riskprofile.ReadyAgain',
    'riskprofile.riskprofile.RiskScoreCalculated',
    'riskprofile.riskprofile.RiskScoreCalculationFailed',
    'riskprofile.riskprofile.RiskScoreCalculationInitiated',
    'riskprofile.riskprofile.RiskScoreUpdated',
    'riskprofile.riskprofile.SetToActive',
    'riskprofile.riskprofile.SetToInactive',
    'riskprofile.riskprofile.UserMarkedAsPoliticallyExposedPerson',
    'riskprofile.riskprofile.UserProjectionUpdated',
    'riskprofile.riskprofile.UserUnmarkedAsPoliticallyExposedPerson',
    'ruleorchestrator.evaluationcase.CaseClosed',
    'ruleorchestrator.evaluationcase.CaseCreated',
    'ruleorchestrator.evaluationcase.CaseSetToInProgress',
    'ruleorchestrator.evaluationcase.RuleHitAppended',
    'ruleorchestrator.ruledefinition.ActionActivated',
    'ruleorchestrator.ruledefinition.ActionDeactivated',
    'ruleorchestrator.ruledefinition.AddedToGroup',
    'ruleorchestrator.ruledefinition.RemovedFromGroup',
    'ruleorchestrator.ruledefinition.RuleDefinitionRegistered',
    'ruleorchestrator.ruledefinition.RuleDefinitionStatusChanged',
    'ruleorchestrator.rulehit.ActionApplied',
    'ruleorchestrator.rulehit.ActionFailed',
    'ruleorchestrator.rulehit.RuleHitEvaluated',
    'ruleorchestrator.rulehit.RuleHitRegistered',
    'ruleorchestrator.rulehit.UnderEvaluation',
    'savings_overview.savings_account.SavingsAccountAdded',
    'savings_overview.savings_account.SavingsAccountRemoved',
    'share_it.group.GroupCreated',
    'share_it.group.GroupExpenseCreated',
    'share_it.group.GroupExpenseDeleted',
    'share_it.group.GroupPaymentCreated',
    'share_it.group.WeShareGroupImported',
    'share_it.payout.SettlementCreated',
    'share_it.user.PhoneNumberInvited',
    'share_it.user.UserIdentityValidated',
    'share_it.user.UserJoinedGroup',
    'share_it.user.UserLeftGroup',
    'share_it.user.UserOnboarded',
    'share_it.user.UserPayoutAccountNumberChanged',
    'standingorder.agreement.StandingOrderAgreementCreated',
    'standingorder.agreement.StandingOrderAgreementDeleted',
    'standingorder.agreement.StandingOrderAgreementUpdated',
    'standingorder.agreement.created',
    'standingorder.agreement.deleted',
    'subaio.payment.PaymentCreated',
    'subaio.payment.PaymentUpdated',
    'subaio.payment.UpcomingPaymentCreated',
    'subaio.subscription.SubscriptionCreated',
    'subaio.subscription.SubscriptionUpdated',
    'subaio.transaction.SubscriptionIDUpdated',
    'subaio.transaction.TransactionMatchesSubscription',
    'subaio.upcoming_payment.UpcomingPaymentCreated',
    'subaio.upcoming_payment.UpcomingPaymentDeleted',
    'subaio.upcoming_payment.UpcomingPaymentUpdated',
    'suspension.userrelationsuspension.userRelationSuspended',
    'suspension.userrelationsuspension.userRelationUnsuspended',
    'swift.file_act.SAAReceived',
    'swift.file_act.Step2Received',
    'swift.file_act_ack.XMLv2Received',
    'swift.fin.MTReceived',
    'swift.fin_ack.MTReceived',
    'term_deposit.savings_account.SavingsAccountCloseRequested',
    'term_deposit.savings_account.SavingsAccountClosed',
    'term_deposit.savings_account.SavingsAccountCreated',
    'term_deposit.savings_account.SavingsAccountInterestApplied',
    'term_deposit.savings_account.SavingsAccountLocked',
    'term_deposit.transfer.TransferFailed',
    'tieredinterest.interest_rate_for_user_updated.InterestRateForUserUpdated',
    'tieredinterest.interest_rate_synced.InterestRateSynced',
    'tieredinterest.new_interest_rate_threshold_created.NewInterestRateThresholdCreated',
    'transaction_bucket.bucket.TransactionBucketRemoved',
    'transaction_bucket.bucket.TransactionBucketUpdated',
    'transaction_card_payments_itl.card_payment.CardAuthorizationEnriched',
    'transaction_card_payments_itl.card_payment.CardAuthorizationFailed',
    'transaction_card_payments_itl.card_payment.FundsCaptured',
    'transaction_category.custom_category.CustomCategoryAssigned',
    'transaction_label.custom_category.CustomCategoryAssigned',
    'transaction_label.custom_category.CustomCategoryCreated',
    'transaction_label.custom_category.CustomCategoryDeleted',
    'transaction_label.custom_category.CustomCategoryUnAssigned',
    'transaction_label.custom_category.CustomCategoryUnassigned',
    'transaction_label.custom_category.CustomCategoryUpdated',
    'transaction_user_content.reaction.ReactionDeleted',
    'transaction_user_content.reaction.ReactionUpdated',
    'transfer.payment.PaymentCreated',
    'unsecured_loan_application.loanapplication.LoanApprovedEvent',
    'user.user.DeletedEvent',
    'user_relation.userrelation.UserRelationCreated',
    'userrelation.userrelation.UserRelationActivated',
    'userrelation.userrelation.UserRelationActivatedV2',
    'userrelation.userrelation.UserRelationActivatedV3',
    'userrelation.userrelation.UserRelationClosed',
    'userrelation.userrelation.UserRelationClosedV3',
    'userrelation.userrelation.UserRelationCreated',
    'userrelation.userrelation.UserRelationCreatedV2',
    'userrelation.userrelation.UserRelationCreatedV3',
    'userrelation.userrelation.UserRelationInactivated',
    'userrelation.userrelation.UserRelationShortNameUpdated',
    'userrelation.userrelation.UserRelationSignedUp',
    'userrelation.userrelation.UserRelationSignedUpV2',
    'userrelation.userrelation.UserRelationSignedUpV3',
    'userrelation.userrelation.UserRelationTypeUpdated',
    'vault_payment_account_interest.interest_posting.PostingCreated',
    'vault_user.vault_user.VaultMappingCreatedEvent',
    'vaultuser.vaultuser.VaultMappingCreatedEvent',
]
#
for topic in topics:
    delete_data(topic, date(2023,2,14))


