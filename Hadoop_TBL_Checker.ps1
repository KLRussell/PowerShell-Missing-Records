# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Global Variable Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

#Global Network Variables
$Global:Settings_HadoopDSN="";
$Global:Settings_Server="";
$Global:Settings_Database="";

# Global Variables
$Global:Settings_TblIDName="";
$Global:Settings_TBLName="";
$Global:Settings_ODSSQLArg="Invoice_Date = eomonth(getdate())";
$Global:Settings_MissingHadoopIDs_Filepath = "";
$Global:Settings_MissingIDs_Filepath = "";
$Global:Settings_ErrorLog_Filepath = "";

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Global Variable End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Control Flow for Script Continuation Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

[boolean]$Global:Var_ContinueExec = $false;

if ($Global:Var_NumChkList -ne $null)
{
    ClassValidation;
}

if ($Global:Var_NumChkList -eq $null)
{
    [boolean]$Global:Var_HeaderCreated;
    [int]$Global:Var_MaxTblID;
    $Global:Var_NumChkList;
}
else
{
    $Global:Var_ContinueExec = $true;
}

Function ClassValidation
{
    foreach ($item in $Global:Var_NumChkList)
    {
        if ($item.TBLName -eq $null -or $item.TBLIDName -eq $null)
        {
            $Global:Var_NumChkList = $Global:Var_NumChkList -ne $item;
        }
        elseif ($item.TBLName -ne $Global:Settings_TBLName -or $item.TBLIDName -ne $Global:Settings_TblIDName)
        {
            
            Remove-Variable Var_* -ErrorAction SilentlyContinue;
            return;
        }
        elseif ($item.NumChkList.Start -eq $null -or $item.NumChkList.End -eq $null -or $item.NumChkList.Flag -eq $null)
        {
            $Global:Var_NumChkList = $Global:Var_NumChkList -ne $item;
        }
        else
        {
            for ($i = 2; $i -lt 7; $i++)
            {
                ClassSubCleanup -NumChkObj $item -Block $i
            }
        }
    }

    return;
}

function ClassSubCleanup
{
    param
    (
        [Parameter(Mandatory=$true)]$NumChkObj,
        [Parameter(Mandatory=$true)][int]$Block
    )

    $SubClass = $(BlockListArr -NumChkObj $NumChkObj -Block $Block);

    foreach ($item in $SubClass)
    {
        if ($item.Start -eq $null -or $item.End -eq $null -or $item.Flag -eq $null)
        {
            $NumChkObj = $NumChkObj -ne $item;
        }
    }
}

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Control Flow for Script Continuation End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Class Block Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

<#
    Class List for errors found between blocks. Error List will store more finite block for processing
#>
class NumChkList
{
    [String]$TBLName;
    [String]$TBLIDName;
    [NumBlock]$NumChkList;
    [int]$BlockType;
    [array]$TenMillion;
    [array]$OneMillion;
    [array]$OneHundredThousand;
    [array]$TenThousand;
    [array]$OneThousand;

    NumChkList() {}

    NumChkList([String]$TBLName, [String]$TBLIDName, [int]$BlockType, [int]$Start, [int]$End, [boolean]$Flag)
    {
        $this.TBLName = $TBLName;
        $this.TBLIDName = $TBLIDName;
        $this.BlockType = $BlockType;
        $this.NumChkList = [NumBlock]::New($Start, $End, $Flag);
    }

    AppendList([int]$BlockType, [int]$Start, [int]$End, [boolean]$Flag)
    {
        switch ($BlockType)
        {
            2
            {
                $this.TenMillion += [NumBlock]::New($Start, $End, $Flag);
            }
            3
            {
                $this.OneMillion += [NumBlock]::New($Start, $End, $Flag);
            }
            4
            {
                $this.OneHundredThousand += [NumBlock]::New($Start, $End, $Flag);
            }
            5
            {
                $this.TenThousand += [NumBlock]::New($Start, $End, $Flag);
            }
            6
            {
                $this.OneThousand += [NumBlock]::New($Start, $End, $Flag);
            }

        }
    }
}

<#
    Class works in conjuncture with NumChkList. This stores number range for different denomination
#>
class NumBlock
{
    [int]$Start;
    [int]$End;
    [boolean]$Flag;
    
    NumBlock() {}

    NumBlock ([int]$Start, [int]$End, [boolean]$Flag)
    {
        $this.Start = $Start;
        $this.End = $End;
        $this.Flag = $Flag;
    }
}

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Class Block End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Network Information Block Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

<#
    Query SQL Server (Aka ODS)
#>
FUNCTION Query
{
    param
    (
        [Parameter(Mandatory=$true)]$SQL,
        [Parameter(Mandatory=$false)][boolean]$Retry
    )

    $error.clear();

    Try
    {
        $Data = Invoke-Sqlcmd -ServerInstance $Global:Settings_Server -Database $Global:Settings_Database -QueryTimeout 0 -Query $SQL -ErrorAction SilentlyContinue -OutputSqlErrors $false;
    }
    Catch
    {
        if ($Retry)
        {
            write-host "Error! Failed to query $Global:Settings_HadoopDSN connection. Writing to error log";
            Add-Content $Global:Settings_ErrorLog_Filepath "$Global:Settings_Server -> $sql";
            Return $null;
        }
        else
        {
            write-host "Warning! $Global:Settings_Server Query failed. Retrying same query in 1 minutes";
            Start-Sleep -Seconds 60;
            write-host "Re-attempting query again...";
            Return (Query -SQL $SQL -Retry $true);
        }
    }

    If ($Data -Eq $null -or [string]::IsNullOrEmpty($Data) -or $Data.Count -Eq 0)
    {
        Return $null;
    }
    else
    {
        Return $Data;
    }
}

<#
    Query ODBC Connection (Aka Hadoop)
        Make sure that 64-bit ODBC drivers on 64-bit systems has this Powershell script run in 64-bit Powershell
#>
function Hquery
{
    param
    (
        [Parameter(Mandatory=$true)]$SQL,
        [Parameter(Mandatory=$false)][boolean]$Retry
    )

    $error.clear();

    $conn = "DSN=$Global:Settings_HadoopDSN;DATABASE=default;Trusted_Connection=Yes;";
    $data = New-Object System.Data.DataSet;

    Try
    {
        (New-Object System.Data.Odbc.OdbcDataAdapter($SQL, $conn)).Fill($data) | out-null;
    }
    Catch
    {
        if ($Retry)
        {
            write-host "Error! Failed to query $Global:Settings_HadoopDSN connection. Writing to error log";
            Add-Content $Global:Settings_ErrorLog_Filepath "$Global:Settings_HadoopDSN -> $sql";
            Return $null;
        }
        else
        {
            write-host "Warning! $Global:Settings_HadoopDSN Query failed. Retrying same query in 1 minutes";
            Start-Sleep -Seconds 60;
            write-host "Re-attempting query again...";
            Return $(Hquery -SQL $SQL -Retry $true);
        }
    }

    if ($Data.Tables[0].Rows.Count -eq 0)
    {
        Return $null;
    }
    else
    {
        Return $Data;
    }
}

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Network Information Block End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- General Functions Block Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

<#
    Finds max table ID for table that is being checked for missing records
#>
Function MaxTblID()
{
    if ($Global:Settings_ODSSQLArg -eq $null)
    {
        $sql = "select max($($Global:Settings_TblIDName)) Max_Tbl_ID from $($Global:Settings_TBLName)";
    }
    else
    {
        $sql = "select max($($Global:Settings_TblIDName)) Max_Tbl_ID from $($Global:Settings_TBLName) where $($Global:Settings_ODSSQLArg)";
    }

    return $(query($sql)).Max_Tbl_ID;
}

<#
    Grabs sub list from the Class Obj NumChkList according to block
#>
Function BlockListArr
{
    param
    (
        [Parameter(Mandatory=$true)]$NumChkObj,
        [Parameter(Mandatory=$true)][int]$Block
    )

    switch ($block)
    {
        2
        {
            return $NumChkObj.TenMillion;
        }
        3
        {
            return $NumChkObj.OneMillion;
        }
        4
        {
            return $NumChkObj.OneHundredThousand;
        }
        5
        {
            return $NumChkObj.TenThousand;
        }
        6
        {
            return $NumChkObj.OneThousand;
        }
    }
}

<#
    Grabs list count of sub list from the Class Obj NumChkList according to block
#>
Function BlockListCount()
{
    [OutputType([int])]
    param
    (
        [Parameter(Mandatory=$true)][int]$Block
    )

    [int]$NumOfItems = 0;
    $BlockObj;

    foreach ($NumChkObj in $Global:Var_NumChkList)
    {
        if ($NumChkObj.BlockType -eq ($Block - 1))
        {
            if ($NumChkObj.NumChkList.Flag)
            {
                $NumOfItems++;
            }
        }
        else
        {
            $BlockObjList = $(BlockListArr -NumChkObj $NumChkObj -Block ($Block - 1));

            if ($BlockObjList -ne $null)
            {
                foreach ($item in $BlockObjList)
                {
                    if ($item.Flag)
                    {
                        $NumOfItems++;
                    }
                }
            }
        }
    }

    return [int]$NumOfItems;
}

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- General Functions Block End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Core Functions Block Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

<#
    Initiation of checking blocks of numbers by block
    This will create a list of classes or resume a previous process according to existing variables
#>
Function CheckBlock
{
    param
    (
        [Parameter(Mandatory=$true)][int]$block
    )

    [int]$divisor;
    [string]$BlockName;
    [string]$PrevBlockName;

    [int]$MinVal;
    [int]$MaxVal;
    [string]$SQL;

    switch ($block)
    {
        1
        {
            $BlockName = "One Hundred Million";
            $divisor = 100000000;
        }
        2
        {
            $PrevBlockName = "One Hundred Million";
            $BlockName = "Ten Million";
            $divisor = 10000000;
        }
        3
        {
            $PrevBlockName = "Ten Million";
            $BlockName = "One Million";
            $divisor = 1000000;
        }
        4
        {
            $PrevBlockName = "One Million";
            $BlockName = "One Hundred Thousand";
            $divisor = 100000;
        }
        5
        {
            $PrevBlockName = "One Hundred Thousand";
            $BlockName = "Ten Thousand";
            $divisor = 10000;
        }
        6
        {
            $PrevBlockName = "Ten Thousand";
            $BlockName = "One Thousand";
            $divisor = 1000;
        }
        7
        {
            $BlockName = "Zero to One Thousand";
            $divisor = 1000;
        }
        default
        {
            write-host "Error! CheckNumRange Function had an invalid block passed as parameter";
            exit;
        }
    }

    if ($([math]::floor($Global:Var_MaxTblID / $divisor)) -lt 1)
    {
        return;
    }

    Write-Host "-- Checking $($BlockName)'s Block --";

    if ($Global:Var_NumChkList -eq $null)
    {
        CreateNewClass -Block $Block -BlockName $BlockName -Divisor $Divisor;
    }
    else
    {
        if ($Global:Var_NumChkList.count -lt [math]::Ceiling($Global:Var_MaxTblID / $Divisor) -and -not $Global:Var_HeaderCreated)
        {
            CreateNewClass -Block $Block -BlockName $BlockName -Divisor $Divisor;
        }
        else
        {
            if ($Global:Var_HeaderCreated -and $Block -gt 1)
            {
                ProcessClass -Block $Block -PrevBlockName $PrevBlockName -BlockName $BlockName -Divisor $Divisor;
            }
        }
    }
}

<#
    This will create a list of class objects that has ranges of numbers to be checked by the algorithm
#>
Function CreateNewClass
{
    param
    (
        [Parameter(Mandatory=$true)][int]$Block,
        [Parameter(Mandatory=$true)][String]$BlockName,
        [Parameter(Mandatory=$true)][int]$Divisor
    )

    [int]$ForStartNum;

    if ($Global:Var_ContinueExec)
    {
        $ForStartNum = $Global:Var_NumChkList.count;
    }
    else
    {
        $ForStartNum = 0;
    }

    for ($q = $ForStartNum; $q -lt [math]::Ceiling($Global:Var_MaxTblID / $divisor); $q++)
    {
        $Skip = $false;
        $MinVal = $(($q * $divisor) + 1);
        $MaxVal = $(($q + 1) * $divisor);
        
        if ($MaxVal -gt $Global:Var_MaxTblID)
        {
            $MaxVal = $Global:Var_MaxTblID
        }

        Write-Progress -status "Processing" -Activity "Checking records for $($Global:Settings_TBLName) in Hadoop Cluster between $MinVal to $MaxVal ($q out of $([math]::Ceiling($Global:Var_MaxTblID / $divisor)))" -PercentComplete (100 * ($q /$([math]::Ceiling($Global:Var_MaxTblID / $divisor)))) -Id 1;

        if (Get-Variable 'data' -Scope Global -ErrorAction 'Ignore')
        {
            Clear-Item Variable:data;
        }

        $SQL = "select count(*) from $($Global:Settings_TBLName) where $($Global:Settings_TblIDName) between $($MinVal) and $($MaxVal)";
        $data = hquery($SQL);

        if ($data -ne $null)
        {
            if ($data.tables[0].rows[0][0] -ne $(($MaxVal - $MinVal) + 1))
            {
                write-host "Marking $($BlockName)'s Block $MinVal to $MaxVal for further investigation";            
                [array]$Global:Var_NumChkList += [NumChkList]::New($($Global:Settings_TBLName), $($Global:Settings_TblIDName), $Block, $MinVal, $MaxVal, $true);
                continue;
            }
        }

        [array]$Global:Var_NumChkList += [NumChkList]::New($($Global:Settings_TBLName), $($Global:Settings_TblIDName), $Block, $MinVal, $MaxVal, $false);
    }

    $Global:Var_HeaderCreated = $true;
    Write-Progress -completed $true -id 1;
}

<#
    This will process a range of numbers according to subclasses that denominates according to divisor
    Divisor goes from One Hundred Million -> Ten Million -> One Million -> One Hundred Thousand -> Ten Thousand -> One Thousand
#>
Function ProcessClass
{
    param
    (
        [Parameter(Mandatory=$true)][int]$Block,
        [Parameter(Mandatory=$false)][String]$PrevBlockName,
        [Parameter(Mandatory=$true)][String]$BlockName,
        [Parameter(Mandatory=$true)][int]$Divisor
    )

    $BlockObjList;
    [int]$j = 1;
    
    $total = BlockListCount($Block);

    foreach ($NumChkObj in $Global:Var_NumChkList)
    {
        if ($NumChkObj.BlockType -eq ($Block - 1))
        {
            if ($NumChkObj.NumChkList.Flag)
            {
                Write-Progress -Activity "Checking $($PrevBlockName)'s List $j of $($total[1])" -PercentComplete (100 * ($j++/$total[1])) -Id 1;
                CheckSection -NumChkObj $NumChkObj -Block $Block -StartNum $($NumChkObj.NumChkList.Start) -EndNum $($NumChkObj.NumChkList.End) -divisor $divisor -BlockName $BlockName;
            }
        }
        else
        {
            $BlockObjList = $(BlockListArr -NumChkObj $NumChkObj -Block ($Block - 1));

            if ($BlockObjList -ne $null)
            {
                foreach ($item in $BlockObjList)
                {

                    if ($item.Flag)
                    {
                        if ($Block -eq 7)
                        {
                            Write-Progress -Activity "Checking $($BlockName)'s List $j of $($total[1])" -PercentComplete (100 * ($j++/$total[1])) -Id 1;
                            SearchSection -NumChkObj $NumChkObj -Block $Block -StartNum $item.Start -EndNum $item.End -divisor $divisor -BlockName $BlockName -ListItem $item
                        }
                        else
                        {
                            Write-Progress -Activity "Checking $($PrevBlockName)'s List $j of $($total[1])" -PercentComplete (100 * ($j++/$total[1])) -Id 1;
                            CheckSection -NumChkObj $NumChkObj -Block $Block -StartNum $item.Start -EndNum $item.End -divisor $divisor -BlockName $BlockName;
                        }
                    }
                }
            }
        }
    }

    Write-Progress -completed $true -id 1
}

<#
    Checks a block of numbers by a specific number section to see if missing numbers are within
        If entire section is missing, script will write number range in file log
        Block of numbers is according to ProcessClass function
#>
Function CheckSection
{
    param
    (
        [Parameter(Mandatory=$true)]$NumChkObj,
        [Parameter(Mandatory=$true)][int]$Block,
        [Parameter(Mandatory=$true)][int]$StartNum,
        [Parameter(Mandatory=$true)][int]$EndNum,
        [Parameter(Mandatory=$true)][int]$Divisor,
        [Parameter(Mandatory=$true)][string]$BlockName
    )

    if ($StartNum -gt 0 -and $EndNum -gt 0)
    {
        if ($Global:Var_ContinueExec)
        {
             $BlockObjList = $(BlockListArr -NumChkObj $NumChkObj -Block $Block);
        }

        for ($q = $([math]::floor(($StartNum - 1) / $divisor)); $q -lt $([math]::Ceiling($EndNum / $divisor)); $q++)
        {
            $MinVal = $(($q * $divisor) + 1);
            $MaxVal = $(($q + 1) * $divisor);

            if ($MaxVal -gt $EndNum)
            {
                $MaxVal = $EndNum
            }

            Write-Progress -Activity "Checking records for $($Global:Settings_TBLName) in Hadoop Cluster ($($BlockName)'s Block) between $MinVal to $MaxVal" -PercentComplete (100 * ((($q - $([math]::floor(($StartNum - 1) / $divisor))) + 1)/($([math]::Ceiling($EndNum / $divisor)) - $([math]::floor(($StartNum - 1) / $divisor))))) -Id 2;

            if ($Global:Var_ContinueExec -and $BlockObjList -ne $null)
            {
                $ContinueFlag = $false;

                foreach ($item in $BlockObjList)
                {
                    if ($item.Start -eq $minVal -and $item.End -eq $maxVal)
                    {
                        $ContinueFlag = $true;
                        break;
                    }
                }

                if ($ContinueFlag)
                {
                    continue;
                }
            }

            if (Get-Variable 'data' -Scope Global -ErrorAction 'Ignore')
            {
                Clear-Item Variable:data;
            }

            $SQL = "select count(*) from $($Global:Settings_TBLName) where $($Global:Settings_TblIDName) between $($MinVal) and $($MaxVal)";
            $data = hquery($SQL);

            if ($data -ne $null)
            {
                if ($data.tables[0].rows[0][0] -ne $(($MaxVal - $MinVal) + 1))
                {
                    if ($data.tables[0].rows[0][0] -eq 0)
                    {
                        write-host "Missing Records! Searching IDs from $($Global:Settings_TBLName) in Hadoop Cluster between $MinVal to $MaxVal";

                        if (Get-Variable 'data2' -Scope Global -ErrorAction 'Ignore')
                        {
                            Clear-Item Variable:data2;
                        }

                        $sql = "select $($Global:Settings_TblIDName) from $($Global:Settings_TBLName) where $($Global:Settings_TblIDName) between $MinVal and $MaxVal";
                        $data2 = query($sql);
                        
                        if ($data2 -ne $null)
                        {
                            Add-Content $Global:Settings_MissingHadoopIDs_Filepath $($data2).$($Global:Settings_TblIDName);
                        }
                        else
                        {
                            Add-Content $Global:Settings_MissingIDs_Filepath "($($BlockName)'s Block) Missing Records! Searching IDs from $($Global:Settings_TBLName) in Hadoop Cluster between $MinVal to $MaxVal";
                        }
                    }
                    else
                    {
                        write-host "Marking $($BlockName)'s Block $MinVal to $MaxVal for further investigation";
                        $NumChkObj.AppendList($Block, $MinVal, $MaxVal, $true);
                        continue;
                    }
                }
            }
            $NumChkObj.AppendList($Block, $MinVal, $MaxVal, $false);
        }

        Write-Progress -completed $true -id 2
    }
    else
    {
        write-host "Error! (Start: $startNum or End: $endNum) is either non-existing or invalid number"
        exit;
    }
}

<#
    This section is executed when the One Thousand list is populated
        Missing records in Hadoop will be marked in one log whereas missing in both ODS and Hadoop will be in a different log
#>
Function SearchSection
{
    param
    (
        [Parameter(Mandatory=$true)]$NumChkObj,
        [Parameter(Mandatory=$true)][int]$Block,
        [Parameter(Mandatory=$true)][int]$StartNum,
        [Parameter(Mandatory=$true)][int]$EndNum,
        [Parameter(Mandatory=$true)][int]$Divisor,
        [Parameter(Mandatory=$true)][string]$BlockName,
        [Parameter(Mandatory=$true)]$ListItem
    )
    if ($StartNum -gt 0 -and $EndNum -gt 0)
    {
        if (Get-Variable 'data' -Scope Global -ErrorAction 'Ignore')
        {
            Clear-Item Variable:data;
        }

        $SQL = "select $($Global:Settings_TblIDName) from $($Global:Settings_TBLName) where $($Global:Settings_TblIDName) between $($StartNum) and $($EndNum)";
        $data = hquery($SQL);

        if ($data -ne $null)
        {
            if (Get-Variable 'numbers' -Scope Global -ErrorAction 'Ignore')
            {
                Clear-Item Variable:numbers;
            }

            $numbers = @();

            for ($j = $StartNum; $j -lt $EndNum + 1; $j++)
            {
                $ItemFnd = $false;
                foreach ($Seed in $data.tables[0].rows)
                {
                    if ($seed[0] -eq $j)
                    {
                        $ItemFnd = $true;
                        break;
                    }
                }
                if (-not $ItemFnd)
                {
                    $numbers += $j;
                }
            }

            if (Get-Variable 'data2' -Scope Global -ErrorAction 'Ignore')
            {
                Clear-Item Variable:data2;
            }

            $sql = "select $($Global:Settings_TblIDName) from $($Global:Settings_TBLName) where $($Global:Settings_TblIDName) in ($($numbers -join ','))";

            $data2 = query($sql);

            if ($data2 -ne $null)
            {
                Add-Content $Global:Settings_MissingHadoopIDs_Filepath $($data2).$($Global:Settings_TblIDName);
            }
            else
            {
                Add-Content $Global:Settings_MissingIDs_Filepath $numbers;
            }

            $ListItem.Flag = $false;
        }
    }
    else
    {
        write-host "Error! (Start: $startNum or End: $endNum) is either non-existing or invalid number";
        exit
    }
}

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Core Functions Block End
# --------------------------------------------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Main Loop Start
# --------------------------------------------------------------------------------------------------------------------------------------------------

write-host "Starting Table Integrity Script by Kevin Russell";

if (-not $Global:Var_ContinueExec)
{
    write-host "Finding Max Table ID for $($Global:Settings_TBLName)";
    $Global:Var_MaxTblID = MaxTblID;
    write-host "Max Table ID is $Global:Var_MaxTblID";
}

for ($i = 1; $i -lt 8; $i++)
{
    $process = CheckBlock -Block $i;
}

Write-host "Finished! Please see respective logs for data analysis";

# --------------------------------------------------------------------------------------------------------------------------------------------------
# --------- Main Loop End
# --------------------------------------------------------------------------------------------------------------------------------------------------
