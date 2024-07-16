namespace Pansynchro.Core.Errors;
public class ErrorManager
{
	private static bool continueOnError = false;
	public static bool ContinueOnError { get { return continueOnError; } }
	public static void DisableContinueOnError() { continueOnError = false; }
	public static void EnableContinueOnError() { continueOnError = true; }

}
