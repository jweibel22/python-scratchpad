
import boto3
from lunarway.hour_partition import parse_hour_partition, ParseException
from datetime import date

s3 = boto3.resource('s3')

bucket_name = 'lunarway-prod-data-structured-datalake'
bucket = s3.Bucket(bucket_name)
source = 'lw-go-events'


def move(source, destination):
    print(f"Moving {source} to {destination}")
    s3.Object(bucket_name, destination).copy_from(CopySource=f"{bucket_name}/{source}")
    s3.Object(bucket_name, source).delete()


def process_data(topic):
    prefix = f"source={source}/topic={topic}/day=1970-01-01"

    for obj in list(bucket.objects.filter(Prefix=prefix)):
        try:
            partition = parse_hour_partition(obj.key)
            filename = obj.key.split("/")[-1]
            new_path = f"source={partition.source}/topic={partition.topic}/day=1970-01-01/{filename}"
            move(obj.key, new_path)
        except ParseException:
            pass


# def delete_data(topic):
#     prefix = f"source={source}/topic={topic}/day=2020-01-14"
#
#     for obj in list(bucket.objects.filter(Prefix=prefix)):
#         try:
#             parse_hour_partition(obj.key)
#             print(f"deleting {obj.key}")
#             obj.delete()
#         except ParseException:
#             pass

topics = [
# 'account.AccountNemKontoUpdated',
# 'account.BalanceUpdated',
# 'closeAccount.CloseAccountInitiatedBySupport',
# 'closeAccount.CloseAccountProcessedBySupport',
# 'credit.CreditApproved',
# 'credit.CreditCancellationApproved',
# 'credit.CreditCancellationRejected',
# 'credit.CreditCancellationRequestedByUser',
# 'credit.CreditProductChangeApproved',
# 'credit.CreditProductChangeCancelled',
# 'credit.CreditRejected',
# 'credit.CreditRequestedByUser',
# 'financemanager.BudgetDeleted',
# 'financemanager.BudgetSet',
# 'goal.GoalDeposit',
# 'goal.GoalWithdraw',
# 'insurance.InsuranceCancelled',
# 'insurance.InsuranceClosed',
# 'insurance.InsuranceDeleted',
# 'insurance.PurchaseCompleted',
# 'insurance.PurchaseInitiated',
# 'insurance.SubtypeModificationCompleted',
# 'insurance.SubtypeModificationInitiated',
# 'invest.AccountBalanceUpdated',
# 'invest.ApplicationApproved',
# 'invest.ApplicationDeclined',
# 'invest.ApplicationStarted',
# 'invest.ApplicationSubmitted',
# 'invest.BuyOrderCancelled',
# 'invest.BuyOrderCompleted',
# 'invest.BuyOrderCreated',
# 'invest.FundingDepositCompleted',
# 'invest.FundingDepositSubmitted',
# 'invest.FundingWithdrawalCompleted',
# 'invest.PortfolioUpdated',
# 'invest.PricingUpdated',
# 'invest.SellOrderCancelled',
# 'invest.SellOrderCompleted',
# 'invest.SellOrderCreated',
# 'loan.LoanApplicationAccepted',
# 'loan.LoanApplicationCancelled',
# 'loan.LoanApplicationCompleted',
# 'loan.LoanApplicationDeclined',
# 'loan.LoanApplicationPendingEskat',
# 'loan.LoanApplicationStarted',
# 'loan.LoanApplicationSubmitted',
# 'product.ProductNotifyMeUpdated',
# 'signup.SignupAccepted',
# 'signup.SignupApplied',
# 'signup.SignupApproved',
# 'signup.SignupRedo',
# 'signup.SignupRejected',
# 'signup.SignupStarted',
# 'subscription.InvoicePaymentAttemptFailed',
# 'subscription.InvoicePaymentSucceeded',
# 'subscription.SubscriptionActivated',
# 'subscription.SubscriptionChanged',
# 'subscription.SubscriptionDeleted',
# 'subscription.SubscriptionPaymentFailed',
'tink.ConnectionCreated',
# 'tracking.EventTrackingTriggered',
# 'tracking.ScreenTrackingTriggered',
# 'tx.view.TransactionCreated',
# 'tx.view.TransactionUpdated',
# 'user.BirthDateUpdated',
# 'user.EventGenderUpdated',
# 'user.StateUpdated',
# 'user.UserClosed',
# 'user.UserCreated',

]

# for topic in topics:
#     process_data(topic)
#
#
